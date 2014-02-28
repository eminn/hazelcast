/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.query;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.impl.IndexImpl;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.parser.ParseException;
import com.hazelcast.query.parser.SQLParser;

import java.io.IOException;
import java.util.*;

import static com.hazelcast.query.Predicates.*;

public class SqlPredicate extends AbstractPredicate implements IndexAwarePredicate {

    private final static long serialVersionUID = 1;

    private transient Predicate predicate;
    private String sql;

    public SqlPredicate(String sql) {
        this.sql = sql;
        predicate = createPredicate(sql);
    }

    public SqlPredicate() {
    }

    public boolean apply(Map.Entry mapEntry) {
        return predicate.apply(mapEntry);
    }

    public boolean isIndexed(QueryContext queryContext) {
        if (predicate instanceof IndexAwarePredicate) {
            return ((IndexAwarePredicate) predicate).isIndexed(queryContext);
        }
        return false;
    }

    public Set<QueryableEntry> filter(QueryContext queryContext) {
        return ((IndexAwarePredicate) predicate).filter(queryContext);
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(sql);
    }

    public void readData(ObjectDataInput in) throws IOException {
        sql = in.readUTF();
        predicate = createPredicate(sql);
    }

    private int getApostropheIndex(String str, int start) {
        return str.indexOf("'", start);
    }

    private int getApostropheIndexIgnoringDoubles(String str, int start) {
        int i = str.indexOf("'", start);
        int j = str.indexOf("'", i + 1);
        //ignore doubles
        while(i == j-1){
            i = str.indexOf("'", j+1);
            j = str.indexOf("'", i+1);
        }
        return i;
    }

    private String removeEscapes(String phrase) {
        return (phrase.length() > 2) ? phrase.replace("''","'") : phrase;
    }

    private Predicate createPredicate(String sql) {
        final SQLParser parser = new SQLParser(sql);
        try {
             return parser.Lexer();
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }

    }

    private void validateOperandPosition(int pos) {
        if (pos < 0) {
            throw new RuntimeException("Invalid SQL: [" + sql + "]");
        }
    }

    private Object toValue(final Object key, final Map<String, String> phrases) {
        final String value = phrases.get(key);
        if (value != null) {
            return value;
        } else if (key instanceof String && ("null".equalsIgnoreCase((String) key))) {
            return IndexImpl.NULL;
        } else {
            return key;
        }
    }

    private String[] toValue(final String[] keys, final Map<String, String> phrases) {
        for (int i = 0; i < keys.length; i++) {
            final String value = phrases.get(keys[i]);
            if (value != null) keys[i] = value;
        }
        return keys;
    }

    private void setOrAdd(List tokens, int position, Predicate predicate) {
        if (tokens.size() == 0) {
            tokens.add(predicate);
        } else {
            tokens.set(position, predicate);
        }
    }

    private Predicate eval(Object statement) {
        if (statement instanceof String) {
            return equal((String) statement, "true");
        } else {
            return (Predicate) statement;
        }
    }

    @Override
    public String toString() {
        return predicate.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SqlPredicate)) {
            return false;
        }

        SqlPredicate that = (SqlPredicate) o;

        return sql.equals(that.sql);
    }

    @Override
    public int hashCode() {
        return sql.hashCode();
    }
}
