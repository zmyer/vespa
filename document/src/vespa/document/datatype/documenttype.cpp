// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include <vespa/document/datatype/documenttype.h>
#include <vespa/document/fieldvalue/document.h>
#include <vespa/vespalib/util/exceptions.h>
#include <vespa/vespalib/util/stringfmt.h>
#include <iomanip>
#include <vespa/log/log.h>
#include <vespa/document/base/exceptions.h>

LOG_SETUP(".document.datatype.document");

using vespalib::IllegalArgumentException;
using vespalib::make_string;
using vespalib::stringref;

namespace document {

IMPLEMENT_IDENTIFIABLE(DocumentType, StructuredDataType);

DocumentType::DocumentType()
{
}

DocumentType::DocumentType(const stringref& name, int32_t id)
    : StructuredDataType(name, id),
      _inheritedTypes(),
      _ownedFields(new StructDataType(name + ".header")),
      _fields(_ownedFields.get()),
      _fieldSets()
{
    if (name != "document") {
        _inheritedTypes.push_back(DataType::DOCUMENT);
    }
}

DocumentType::DocumentType(const stringref& name, int32_t id, const StructDataType& fields)
    : StructuredDataType(name, id),
      _inheritedTypes(),
      _fields(&fields),
      _fieldSets()
{
    if (name != "document") {
        _inheritedTypes.push_back(DataType::DOCUMENT);
    }
}

DocumentType::DocumentType(const stringref& name)
    : StructuredDataType(name),
      _inheritedTypes(),
      _ownedFields(new StructDataType(name + ".header")),
      _fields(_ownedFields.get()),
      _fieldSets()
{
    if (name != "document") {
        _inheritedTypes.push_back(DataType::DOCUMENT);
    }
}

DocumentType::DocumentType(const stringref& name, const StructDataType& fields)
    : StructuredDataType(name),
      _inheritedTypes(),
      _fields(&fields),
      _fieldSets()
{
    if (name != "document") {
        _inheritedTypes.push_back(DataType::DOCUMENT);
    }
}

DocumentType::~DocumentType()
{
}

DocumentType &
DocumentType::addFieldSet(const vespalib::string & name, const FieldSet::Fields & fields)
{
    _fieldSets[name] = FieldSet(name, fields);
    return *this;
}

const DocumentType::FieldSet *
DocumentType::getFieldSet(const vespalib::string & name) const
{
    FieldSetMap::const_iterator it(_fieldSets.find(name));
    return (it != _fieldSets.end()) ? & it->second : NULL;
}

void
DocumentType::addField(const Field& field)
{
    if (_fields->hasField(field.getName())) {
        throw IllegalArgumentException( "A field already exists with name " + field.getName(), VESPA_STRLOC);
    } else if (_fields->hasField(field)) {
        throw IllegalArgumentException(make_string("A field already exists with id %i.", field.getId()), VESPA_STRLOC);
    } else if (!_ownedFields.get()) {
        throw vespalib::IllegalStateException(make_string(
                        "Cannot add field %s to a DocumentType that does not "
                        "own its fields.", field.getName().c_str()), VESPA_STRLOC);
    }
    _ownedFields->addField(field);
}

void
DocumentType::inherit(const DocumentType &docType) {
    if (docType.getName() == "document") {
        return;
    }
    if (docType.isA(*this)) {
        throw IllegalArgumentException(
                "Document type " + docType.toString() + " already inherits type "
                + toString() + ". Cannot add cyclic dependencies.", VESPA_STRLOC);
    }
        // If we already inherits this type, there is no point in adding it
        // again.
    if (isA(docType)) {
            // If we already directly inherits it, complain
        for (std::vector<const DocumentType *>::const_iterator
                it = _inheritedTypes.begin(); it != _inheritedTypes.end(); ++it)
        {
            if (**it == docType) {
                throw IllegalArgumentException(
                        "DocumentType " + getName() + " already inherits "
                        "document type " + docType.getName(), VESPA_STRLOC);
            }
        }
            // Indirectly already inheriting it is oki, as this can happen
            // due to inherited documents inheriting the same type.
        LOG(info, "Document type %s inherits document type %s from multiple "
                  "types.", getName().c_str(), docType.getName().c_str());
        return;
    }
        // Add non-conflicting types.
    Field::Set fs = docType._fields->getFieldSet();
    for (Field::Set::const_iterator it = fs.begin(); it != fs.end(); ++it) {
        if (!_ownedFields.get()) {
            _ownedFields.reset(_fields->clone());
            _fields = _ownedFields.get();
        }
        _ownedFields->addInheritedField(**it);
    }
    // If we inherit default document type Document.0, remove that if adding
    // another parent, as that has to also inherit Document
    if (_inheritedTypes.size() == 1 && *_inheritedTypes[0] == *DataType::DOCUMENT) {
        _inheritedTypes.clear();
    }
    _inheritedTypes.push_back(&docType);
}

bool
DocumentType::isA(const DataType& other) const
{
    for (std::vector<const DocumentType *>::const_iterator
         it = _inheritedTypes.begin(); it != _inheritedTypes.end(); ++it)
    {
        if ((*it)->isA(other)) return true;
    }
    return (*this == other);
}

FieldValue::UP
DocumentType::createFieldValue() const
{
    return FieldValue::UP(new Document(*this, DocumentId("doc::")));
}

void
DocumentType::print(std::ostream& out, bool verbose,
                    const std::string& indent) const
{
    out << "DocumentType(" << getName();
    if (verbose) {
        out << ", id " << getId();
    }
    out << ")";
    if (verbose) {
        if (!_inheritedTypes.empty()) {
            std::vector<const DocumentType *>::const_iterator it(
                    _inheritedTypes.begin());
            out << "\n" << indent << "    : ";
            (*it)->print(out, false, "");
            while (++it != _inheritedTypes.end()) {
                out << ",\n" << indent << "      ";
                (*it)->print(out, false, "");
            }
        }
        out << " {\n" << indent << "  ";
        _fields->print(out, verbose, indent + "  ");
        out << "\n" << indent << "}";
    }
}

bool
DocumentType::operator==(const DataType& other) const
{
    if (&other == this) return true;
    if (!DataType::operator==(other)) return false;
    const DocumentType* o(dynamic_cast<const DocumentType*>(&other));
    if (o == 0) return false;
    if (*_fields != *o->_fields) return false;
    if (_inheritedTypes.size() != o->_inheritedTypes.size()) return false;
    std::vector<const DocumentType *>::const_iterator it1(_inheritedTypes.begin());
    std::vector<const DocumentType *>::const_iterator it2(o->_inheritedTypes.begin());
    while (it1 != _inheritedTypes.end()) {
        if (**it1 != **it2) return false;
        ++it1;
        ++it2;
    }
    return true;
}

const Field&
DocumentType::getField(const stringref& name) const
{
    return _fields->getField(name);
}

const Field&
DocumentType::getField(int fieldId) const
{
    return _fields->getField(fieldId);
}

bool DocumentType::hasField(const stringref &name) const {
    return _fields->hasField(name);
}

bool DocumentType::hasField(int fieldId) const {
    return _fields->hasField(fieldId);
}

Field::Set
DocumentType::getFieldSet() const
{
    return _fields->getFieldSet();
}

DocumentType *
DocumentType::clone() const {
    return new DocumentType(*this);
}

} // document
