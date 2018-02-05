// Copyright 2018 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.searchdefinition.expressiontransforms;

import com.yahoo.search.query.profile.QueryProfileRegistry;
import com.yahoo.searchdefinition.RankProfile;
import com.yahoo.searchlib.rankingexpression.evaluation.Value;
import com.yahoo.searchlib.rankingexpression.transform.TransformContext;

import java.util.Map;

/**
 * Extends the transform context with rank profile information
 *
 * @author bratseth
 */
public class RankProfileTransformContext extends TransformContext {

    private final RankProfile rankProfile;
    private final QueryProfileRegistry queryProfiles;
    private final Map<String, RankProfile.Macro> inlineMacros;
    private final Map<String, String> rankPropertiesOutput;

    public RankProfileTransformContext(RankProfile rankProfile,
                                       QueryProfileRegistry queryProfiles,
                                       Map<String, Value> constants,
                                       Map<String, RankProfile.Macro> inlineMacros,
                                       Map<String, String> rankPropertiesOutput) {
        super(constants);
        this.rankProfile = rankProfile;
        this.queryProfiles = queryProfiles;
        this.inlineMacros = inlineMacros;
        this.rankPropertiesOutput = rankPropertiesOutput;
    }

    public RankProfile rankProfile() { return rankProfile; }
    public QueryProfileRegistry queryProfiles() { return queryProfiles; }
    public Map<String, RankProfile.Macro> inlineMacros() { return inlineMacros; }
    public Map<String, String> rankPropertiesOutput() { return rankPropertiesOutput; }

}
