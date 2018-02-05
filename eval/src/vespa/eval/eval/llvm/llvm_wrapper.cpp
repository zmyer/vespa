// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include <cmath>
#include "llvm_wrapper.h"
#include <vespa/eval/eval/node_visitor.h>
#include <vespa/eval/eval/node_traverser.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/Analysis/Passes.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/LinkAllPasses.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <vespa/eval/eval/check_type.h>
#include <vespa/vespalib/stllike/hash_set.h>
#include <vespa/vespalib/util/approx.h>

double vespalib_eval_ldexp(double a, double b) { return std::ldexp(a, b); }
double vespalib_eval_min(double a, double b) { return std::min(a, b); }
double vespalib_eval_max(double a, double b) { return std::max(a, b); }
double vespalib_eval_isnan(double a) { return (std::isnan(a) ? 1.0 : 0.0); }
double vespalib_eval_approx(double a, double b) { return (vespalib::approx_equal(a, b) ? 1.0 : 0.0); }
double vespalib_eval_relu(double a) { return std::max(a, 0.0); }
double vespalib_eval_sigmoid(double a) { return 1.0 / (1.0 + std::exp(-1.0 * a)); }
double vespalib_eval_elu(double a) { return (a < 0) ? std::exp(a) - 1.0 : a; }

using vespalib::eval::gbdt::Forest;
using resolve_function = double (*)(void *ctx, size_t idx);
double vespalib_eval_forest_proxy(Forest::eval_function eval_forest, const Forest *forest,
                                  resolve_function resolve, void *ctx, size_t num_params)
{
    if (num_params <= 64) {
        double params[64];
        for (size_t i = 0; i < num_params; ++i) {
            params[i] = resolve(ctx, i);
        }
        return eval_forest(forest, &params[0]);
    } else {
        std::vector<double> params;
        params.reserve(num_params);
        for (size_t i = 0; i < num_params; ++i) {
            params.push_back(resolve(ctx, i));
        }
        return eval_forest(forest, &params[0]);
    }
}

namespace vespalib {
namespace eval {

using namespace nodes;

namespace {

struct SetMemberHash : PluginState {
    vespalib::hash_set<double> members;
    explicit SetMemberHash(const In &in) : members(in.num_entries() * 3) {
        for (size_t i = 0; i < in.num_entries(); ++i) {
            members.insert(in.get_entry(i).get_const_value());
        }
    }
    static bool check_membership(const PluginState *state, double value) {
        const SetMemberHash &my_state = *((const SetMemberHash *)state);
        return (my_state.members.find(value) != my_state.members.end());
    }
};

struct FunctionBuilder : public NodeVisitor, public NodeTraverser {

    llvm::LLVMContext        &context;
    llvm::Module             &module;
    llvm::IRBuilder<>         builder;
    std::vector<llvm::Value*> params;
    std::vector<llvm::Value*> values;
    llvm::Function           *function;
    size_t                    num_params;
    PassParams                pass_params;
    bool                      inside_forest;
    const Node               *forest_end;
    const gbdt::Optimize::Chain &forest_optimizers;
    std::vector<gbdt::Forest::UP> &forests;
    std::vector<PluginState::UP> &plugin_state;

    llvm::PointerType *make_eval_forest_funptr_t() {
        std::vector<llvm::Type*> param_types;
        param_types.push_back(builder.getVoidTy()->getPointerTo());
        param_types.push_back(builder.getDoubleTy()->getPointerTo());
        llvm::FunctionType *function_type = llvm::FunctionType::get(builder.getDoubleTy(), param_types, false);
        return llvm::PointerType::get(function_type, 0);
    }

    llvm::PointerType *make_resolve_param_funptr_t() {
        std::vector<llvm::Type*> param_types;
        param_types.push_back(builder.getVoidTy()->getPointerTo());
        param_types.push_back(builder.getInt64Ty());
        llvm::FunctionType *function_type = llvm::FunctionType::get(builder.getDoubleTy(), param_types, false);
        return llvm::PointerType::get(function_type, 0);
    }

    llvm::PointerType *make_eval_forest_proxy_funptr_t() {
        std::vector<llvm::Type*> param_types;
        param_types.push_back(make_eval_forest_funptr_t());
        param_types.push_back(builder.getVoidTy()->getPointerTo());
        param_types.push_back(make_resolve_param_funptr_t());
        param_types.push_back(builder.getVoidTy()->getPointerTo());
        param_types.push_back(builder.getInt64Ty());
        llvm::FunctionType *function_type = llvm::FunctionType::get(builder.getDoubleTy(), param_types, false);
        return llvm::PointerType::get(function_type, 0);
    }

    llvm::PointerType *make_check_membership_funptr_t() {
        std::vector<llvm::Type*> param_types;
        param_types.push_back(builder.getVoidTy()->getPointerTo());
        param_types.push_back(builder.getDoubleTy());
        llvm::FunctionType *function_type = llvm::FunctionType::get(builder.getInt1Ty(), param_types, false);
        return llvm::PointerType::get(function_type, 0);
    }

    FunctionBuilder(llvm::LLVMContext &context_in,
                    llvm::Module &module_in,
                    const vespalib::string &name_in,
                    size_t num_params_in,
                    PassParams pass_params_in,
                    const gbdt::Optimize::Chain &forest_optimizers_in,
                    std::vector<gbdt::Forest::UP> &forests_out,
                    std::vector<PluginState::UP> &plugin_state_out)
        : context(context_in),
          module(module_in),
          builder(context),
          params(),
          values(),
          function(nullptr),
          num_params(num_params_in),
          pass_params(pass_params_in),
          inside_forest(false),
          forest_end(nullptr),
          forest_optimizers(forest_optimizers_in),
          forests(forests_out),
          plugin_state(plugin_state_out)
    {
        std::vector<llvm::Type*> param_types;
        if (pass_params == PassParams::SEPARATE) {
            param_types.resize(num_params_in, builder.getDoubleTy());
        } else if (pass_params == PassParams::ARRAY) {
            param_types.push_back(builder.getDoubleTy()->getPointerTo());
        } else {
            assert(pass_params == PassParams::LAZY);
            param_types.push_back(make_resolve_param_funptr_t());
            param_types.push_back(builder.getVoidTy()->getPointerTo());
        }
        llvm::FunctionType *function_type = llvm::FunctionType::get(builder.getDoubleTy(), param_types, false);
        function = llvm::Function::Create(function_type, llvm::Function::ExternalLinkage, name_in.c_str(), &module);
        function->addFnAttr(llvm::Attribute::AttrKind::NoInline);
        llvm::BasicBlock *block = llvm::BasicBlock::Create(context, "entry", function);
        builder.SetInsertPoint(block);
        for (llvm::Function::arg_iterator itr = function->arg_begin(); itr != function->arg_end(); ++itr) {
            params.push_back(&(*itr));
        }
    }
    ~FunctionBuilder();

    //-------------------------------------------------------------------------

    llvm::Value *get_param(size_t idx) {
        assert(idx < num_params);
        if (pass_params == PassParams::SEPARATE) {
            assert(idx < params.size());
            return params[idx];
        } else if (pass_params == PassParams::ARRAY) {
            assert(params.size() == 1);
            llvm::Value *param_array = params[0];
            llvm::Value *addr = builder.CreateGEP(param_array, builder.getInt64(idx));
            return builder.CreateLoad(addr);
        }
        assert(pass_params == PassParams::LAZY);
        assert(params.size() == 2);
        return builder.CreateCall(params[0], {params[1], builder.getInt64(idx)}, "resolve_param");
    }

    //-------------------------------------------------------------------------

    void push(llvm::Value *value) {
        values.push_back(value);
    }

    void discard() {
        assert(!values.empty());
        values.pop_back();
    }

    llvm::Value *pop_bool() {
        assert(!values.empty());
        llvm::Value *value = values.back();
        values.pop_back();
        if (value->getType()->isIntegerTy(1)) {
            return value;
        }
        assert(value->getType()->isDoubleTy());
        return builder.CreateFCmpUNE(value, llvm::ConstantFP::get(context, llvm::APFloat(0.0)), "as_bool");
    }

    llvm::Value *pop_double() {
        assert(!values.empty());
        llvm::Value *value = values.back();
        values.pop_back();
        if (value->getType()->isDoubleTy()) {
            return value;
        }
        assert(value->getType()->isIntegerTy(1));
        return builder.CreateUIToFP(value, builder.getDoubleTy(), "as_double");
    }

    //-------------------------------------------------------------------------

    bool try_optimize_forest(const Node &item) {
        auto trees = gbdt::extract_trees(item);
        gbdt::ForestStats stats(trees);
        auto optimize_result = gbdt::Optimize::apply_chain(forest_optimizers, stats, trees);
        if (!optimize_result.valid()) {
            return false;
        }
        forests.push_back(std::move(optimize_result.forest));
        void *eval_ptr = (void *) optimize_result.eval;
        gbdt::Forest *forest = forests.back().get();
        llvm::PointerType *eval_funptr_t = make_eval_forest_funptr_t();
        llvm::Value *eval_fun = builder.CreateIntToPtr(builder.getInt64((uint64_t)eval_ptr), eval_funptr_t, "inject_eval");
        llvm::Value *ctx = builder.CreateIntToPtr(builder.getInt64((uint64_t)forest), builder.getVoidTy()->getPointerTo(), "inject_ctx");
        if (pass_params == PassParams::ARRAY) {
	    push(builder.CreateCall(eval_fun, {ctx, params[0]}, "call_eval"));
        } else {
            assert(pass_params == PassParams::LAZY);
            llvm::PointerType *proxy_funptr_t = make_eval_forest_proxy_funptr_t();
            llvm::Value *proxy_fun = builder.CreateIntToPtr(builder.getInt64((uint64_t)vespalib_eval_forest_proxy), proxy_funptr_t, "inject_eval_proxy");
            push(builder.CreateCall(proxy_fun, {eval_fun, ctx, params[0], params[1], builder.getInt64(stats.num_params)}));
        }
        return true;
    }

    //-------------------------------------------------------------------------

    bool open(const Node &node) override {
        if (node.is_const()) {
            push_double(node.get_const_value());
            return false;
        }
        if (!inside_forest && (pass_params != PassParams::SEPARATE) && node.is_forest()) {
            if (try_optimize_forest(node)) {
                return false;
            }
            inside_forest = true;
            forest_end = &node;
        }
        if (check_type<If>(node)) {
            node.accept(*this);
            return false;
        }
        return true;
    }

    void close(const Node &node) override {
        node.accept(*this);
        if (inside_forest && (forest_end == &node)) {
            inside_forest = false;
            forest_end = nullptr;
        }
    }

    //-------------------------------------------------------------------------

    void build_root(const Node &node) {
        node.traverse(*this);
    }

    void build_forest_fragment(const std::vector<const Node *> &trees) {
        inside_forest = true;
        assert(!trees.empty());
        llvm::Value *sum = nullptr;
        for (auto tree: trees) {
            tree->traverse(*this);
            llvm::Value *tree_value = pop_double();
            sum = (sum)
                  ? builder.CreateFAdd(sum, tree_value, "add_tree")
                  : tree_value;
        }
        push(sum);
        inside_forest = false;
    }

    llvm::Function *build() {
        builder.CreateRet(pop_double());
        assert(values.empty());
        llvm::verifyFunction(*function);
        return function;
    }

    //-------------------------------------------------------------------------

    void push_double(double value) {
        push(llvm::ConstantFP::get(builder.getDoubleTy(), value));
    }

    void make_error(size_t num_children) {
        for (size_t i = 0; i < num_children; ++i) {
            discard();
        }
        push_double(error_value);
    }

    void make_call_1(llvm::Function *fun) {
        if (fun == nullptr || fun->arg_size() != 1) {
            return make_error(1);
        }
        llvm::Value *a = pop_double();
        push(builder.CreateCall(fun, a));
    }
    void make_call_1(const llvm::Intrinsic::ID &id) {
        make_call_1(llvm::Intrinsic::getDeclaration(&module, id, builder.getDoubleTy()));
    }
    void make_call_1(const char *name) {
        make_call_1(dynamic_cast<llvm::Function*>(module.getOrInsertFunction(name,
                                builder.getDoubleTy(),
                                builder.getDoubleTy(), nullptr)));
    }

    void make_call_2(llvm::Function *fun) {
        if (fun == nullptr || fun->arg_size() != 2) {
            return make_error(2);
        }
        llvm::Value *b = pop_double();
        llvm::Value *a = pop_double();
        push(builder.CreateCall(fun, {a, b}));
    }
    void make_call_2(const llvm::Intrinsic::ID &id) {
        make_call_2(llvm::Intrinsic::getDeclaration(&module, id, builder.getDoubleTy()));
    }
    void make_call_2(const char *name) {
        make_call_2(dynamic_cast<llvm::Function*>(module.getOrInsertFunction(name,
                                builder.getDoubleTy(),
                                builder.getDoubleTy(),
                                builder.getDoubleTy(), nullptr)));
    }

    //-------------------------------------------------------------------------

    // basic nodes

    void visit(const Number &item) override {
        push_double(item.value());
    }
    void visit(const Symbol &item) override {
        push(get_param(item.id()));
    }
    void visit(const String &item) override {
        push_double(item.hash());
    }
    void visit(const In &item) override {
        llvm::Value *lhs = pop_double();
        if (item.num_entries() > 8) {
            // build call to hash lookup
            plugin_state.emplace_back(new SetMemberHash(item));
            void *call_ptr = (void *) SetMemberHash::check_membership;
            PluginState *state = plugin_state.back().get();
            llvm::PointerType *funptr_t = make_check_membership_funptr_t();
            llvm::Value *call_fun = builder.CreateIntToPtr(builder.getInt64((uint64_t)call_ptr), funptr_t, "inject_call_addr");
            llvm::Value *ctx = builder.CreateIntToPtr(builder.getInt64((uint64_t)state), builder.getVoidTy()->getPointerTo(), "inject_ctx");
            push(builder.CreateCall(call_fun, {ctx, lhs}, "call_check_membership"));
        } else {
            // build explicit code to check all set members
            llvm::Value *found = builder.getFalse();
            for (size_t i = 0; i < item.num_entries(); ++i) {
                llvm::Value *elem = llvm::ConstantFP::get(builder.getDoubleTy(), item.get_entry(i).get_const_value());
                llvm::Value *elem_eq = builder.CreateFCmpOEQ(lhs, elem, "elem_eq");
                found = builder.CreateOr(found, elem_eq, "found");
            }
            push(found);
        }
    }
    void visit(const Neg &) override {
        llvm::Value *child = pop_double();
        push(builder.CreateFNeg(child, "neg_res"));
    }
    void visit(const Not &) override {
        llvm::Value *child = pop_bool();
        push(builder.CreateNot(child, "not_res"));
    }
    void visit(const If &item) override {
        // NB: visit not open
        llvm::BasicBlock *true_block = llvm::BasicBlock::Create(context, "true_block", function);
        llvm::BasicBlock *false_block = llvm::BasicBlock::Create(context, "false_block", function);
        llvm::BasicBlock *merge_block = llvm::BasicBlock::Create(context, "merge_block", function);
        item.cond().traverse(*this); // NB: recursion
        llvm::Value *cond = pop_bool();
        builder.CreateCondBr(cond, true_block, false_block);
        // true block
        builder.SetInsertPoint(true_block);
        item.true_expr().traverse(*this); // NB: recursion
        llvm::Value *true_res = pop_double();
        llvm::BasicBlock *true_end = builder.GetInsertBlock();
        builder.CreateBr(merge_block);
        // false block
        builder.SetInsertPoint(false_block);
        item.false_expr().traverse(*this); // NB: recursion
        llvm::Value *false_res = pop_double();
        llvm::BasicBlock *false_end = builder.GetInsertBlock();
        builder.CreateBr(merge_block);
        // merge block
        builder.SetInsertPoint(merge_block);
        llvm::PHINode *phi = builder.CreatePHI(builder.getDoubleTy(), 2, "if_res");
        phi->addIncoming(true_res, true_end);
        phi->addIncoming(false_res, false_end);
        push(phi);
    }
    void visit(const Error &) override {
        make_error(0);
    }

    // tensor nodes (not supported in compiled expressions)

    void visit(const TensorMap &node) override {
        make_error(node.num_children());
    }
    void visit(const TensorJoin &node) override {
        make_error(node.num_children());
    }
    void visit(const TensorReduce &node) override {
        make_error(node.num_children());
    }
    void visit(const TensorRename &node) override {
        make_error(node.num_children());
    }
    void visit(const TensorLambda &node) override {
        make_error(node.num_children());
    }
    void visit(const TensorConcat &node) override {
        make_error(node.num_children());
    }

    // operator nodes

    void visit(const Add &) override {
        llvm::Value *b = pop_double();
        llvm::Value *a = pop_double();
        push(builder.CreateFAdd(a, b, "add_res"));
    }
    void visit(const Sub &) override {
        llvm::Value *b = pop_double();
        llvm::Value *a = pop_double();
        push(builder.CreateFSub(a, b, "sub_res"));
    }
    void visit(const Mul &) override {
        llvm::Value *b = pop_double();
        llvm::Value *a = pop_double();
        push(builder.CreateFMul(a, b, "mul_res"));
    }
    void visit(const Div &) override {
        llvm::Value *b = pop_double();
        llvm::Value *a = pop_double();
        push(builder.CreateFDiv(a, b, "div_res"));
    }
    void visit(const Mod &) override {
        make_call_2("fmod");
    }
    void visit(const Pow &) override {
        make_call_2(llvm::Intrinsic::pow);
    }
    void visit(const Equal &) override {
        llvm::Value *b = pop_double();
        llvm::Value *a = pop_double();
        push(builder.CreateFCmpOEQ(a, b, "cmp_eq_res"));
    }
    void visit(const NotEqual &) override {
        llvm::Value *b = pop_double();
        llvm::Value *a = pop_double();
        push(builder.CreateFCmpUNE(a, b, "cmp_ne_res"));
    }
    void visit(const Approx &) override {
        make_call_2("vespalib_eval_approx");
    }
    void visit(const Less &) override {
        llvm::Value *b = pop_double();
        llvm::Value *a = pop_double();
        push(builder.CreateFCmpOLT(a, b, "cmp_lt_res"));
    }
    void visit(const LessEqual &) override {
        llvm::Value *b = pop_double();
        llvm::Value *a = pop_double();
        push(builder.CreateFCmpOLE(a, b, "cmp_le_res"));
    }
    void visit(const Greater &) override {
        llvm::Value *b = pop_double();
        llvm::Value *a = pop_double();
        push(builder.CreateFCmpOGT(a, b, "cmp_gt_res"));
    }
    void visit(const GreaterEqual &) override {
        llvm::Value *b = pop_double();
        llvm::Value *a = pop_double();
        push(builder.CreateFCmpOGE(a, b, "cmp_ge_res"));
    }
    void visit(const And &) override {
        llvm::Value *b = pop_bool();
        llvm::Value *a = pop_bool();
        push(builder.CreateAnd(a, b, "and_res"));
    }
    void visit(const Or &) override {
        llvm::Value *b = pop_bool();
        llvm::Value *a = pop_bool();
        push(builder.CreateOr(a, b, "or_res"));
    }

    // call nodes

    void visit(const Cos &) override {
        make_call_1(llvm::Intrinsic::cos);
    }
    void visit(const Sin &) override {
        make_call_1(llvm::Intrinsic::sin);
    }
    void visit(const Tan &) override {
        make_call_1("tan");
    }
    void visit(const Cosh &) override {
        make_call_1("cosh");
    }
    void visit(const Sinh &) override {
        make_call_1("sinh");
    }
    void visit(const Tanh &) override {
        make_call_1("tanh");
    }
    void visit(const Acos &) override {
        make_call_1("acos");
    }
    void visit(const Asin &) override {
        make_call_1("asin");
    }
    void visit(const Atan &) override {
        make_call_1("atan");
    }
    void visit(const Exp &) override {
        make_call_1(llvm::Intrinsic::exp);
    }
    void visit(const Log10 &) override {
        make_call_1(llvm::Intrinsic::log10);
    }
    void visit(const Log &) override {
        make_call_1(llvm::Intrinsic::log);
    }
    void visit(const Sqrt &) override {
        make_call_1(llvm::Intrinsic::sqrt);
    }
    void visit(const Ceil &) override {
        make_call_1(llvm::Intrinsic::ceil);
    }
    void visit(const Fabs &) override {
        make_call_1(llvm::Intrinsic::fabs);
    }
    void visit(const Floor &) override {
        make_call_1(llvm::Intrinsic::floor);
    }
    void visit(const Atan2 &) override {
        make_call_2("atan2");
    }
    void visit(const Ldexp &) override {
        make_call_2("vespalib_eval_ldexp");
    }
    void visit(const Pow2 &) override {
        make_call_2(llvm::Intrinsic::pow);
    }
    void visit(const Fmod &) override {
        make_call_2("fmod");
    }
    void visit(const Min &) override {
        make_call_2("vespalib_eval_min");
    }
    void visit(const Max &) override {
        make_call_2("vespalib_eval_max");
    }
    void visit(const IsNan &) override {
        make_call_1("vespalib_eval_isnan");
    }
    void visit(const Relu &) override {
        make_call_1("vespalib_eval_relu");
    }
    void visit(const Sigmoid &) override {
        make_call_1("vespalib_eval_sigmoid");
    }
    void visit(const Elu &) override {
        make_call_1("vespalib_eval_elu");
    }
};

FunctionBuilder::~FunctionBuilder() { }

} // namespace vespalib::eval::<unnamed>

struct InitializeNativeTarget {
    InitializeNativeTarget() {
        llvm::InitializeNativeTarget();
        llvm::InitializeNativeTargetAsmPrinter();
        llvm::InitializeNativeTargetAsmParser();
    }
    ~InitializeNativeTarget() {
        llvm::llvm_shutdown();
#ifdef HAS_LLVM_DESTROY_STATIC_MUTEX
        llvm::llvm_destroy_static_mutex();
#endif
#ifdef HAS_LLVM_DESTROY_OPENED_HANDLES
        llvm::llvm_destroy_opened_handles();
#endif
    }
} initialize_native_target;

std::recursive_mutex LLVMWrapper::_global_llvm_lock;

LLVMWrapper::LLVMWrapper()
    : _context(),
      _module(),
      _engine(),
      _functions(),
      _forests(),
      _plugin_state()
{
    std::lock_guard<std::recursive_mutex> guard(_global_llvm_lock);
    _context = std::make_unique<llvm::LLVMContext>();
    _module = std::make_unique< llvm::Module>("LLVMWrapper", *_context);
}


size_t
LLVMWrapper::make_function(size_t num_params, PassParams pass_params, const Node &root,
                           const gbdt::Optimize::Chain &forest_optimizers)
{
    std::lock_guard<std::recursive_mutex> guard(_global_llvm_lock);
    size_t function_id = _functions.size();
    FunctionBuilder builder(*_context, *_module,
                            vespalib::make_string("f%zu", function_id),
                            num_params, pass_params,
                            forest_optimizers, _forests, _plugin_state);
    builder.build_root(root);
    _functions.push_back(builder.build());
    return function_id;
}

size_t
LLVMWrapper::make_forest_fragment(size_t num_params, const std::vector<const Node *> &fragment)
{
    std::lock_guard<std::recursive_mutex> guard(_global_llvm_lock);
    size_t function_id = _functions.size();
    FunctionBuilder builder(*_context, *_module,
                            vespalib::make_string("f%zu", function_id),
                            num_params, PassParams::ARRAY,
                            gbdt::Optimize::none, _forests, _plugin_state);
    builder.build_forest_fragment(fragment);
    _functions.push_back(builder.build());
    return function_id;
}

void
LLVMWrapper::compile(bool dump_module)
{
    std::lock_guard<std::recursive_mutex> guard(_global_llvm_lock);
    if (dump_module) {
        _module->dump();
    }
    _engine.reset(llvm::EngineBuilder(std::move(_module)).setOptLevel(llvm::CodeGenOpt::Aggressive).create());
    assert(_engine && "llvm jit not available for your platform");
    _engine->finalizeObject();
}

void *
LLVMWrapper::get_function_address(size_t function_id)
{
    std::lock_guard<std::recursive_mutex> guard(_global_llvm_lock);
    return _engine->getPointerToFunction(_functions[function_id]);
}

LLVMWrapper::~LLVMWrapper() {
    std::lock_guard<std::recursive_mutex> guard(_global_llvm_lock);
    _plugin_state.clear();
    _forests.clear();
    _functions.clear();
    _engine.reset();
    _module.reset();
    _context.reset();
}

} // namespace vespalib::eval
} // namespace vespalib
