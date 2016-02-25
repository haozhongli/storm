/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.tuple;


import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

public class FieldsTest {

    @Test
    public void fieldsConstructorTest() {
        Assert.assertTrue("constructor with (String... fields)", new Fields("foo", "bar") instanceof Fields);

        try {
            new Fields("foo", "bar", "foo");
            Assert.fail("Expected Exception not Thrown");
        } catch (IllegalArgumentException e) {
        }

        Assert.assertTrue("constructor with (List<String> fields)", new Fields(Lists.newArrayList("foo", "bar")) instanceof Fields);
        try {
            new Fields(Lists.newArrayList("foo", "bar", "foo"));
            Assert.fail("Expected Exception not Thrown");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void fieldsMethodsTest() {
        Fields fields = new Fields("foo", "bar");
        // testing size
        Assert.assertEquals("method .size", 2, fields.size());

        // testing get
        Assert.assertEquals("method .get", "foo", fields.get(0));
        Assert.assertEquals("method .get", "bar", fields.get(1));
        try {
            fields.get(2);
            Assert.fail("method .get Expected Exception not Thrown");
        } catch (IndexOutOfBoundsException e) {
        }

        // testing fieldIndex
        Assert.assertEquals("method .fieldIndex", 0, fields.fieldIndex("foo"));
        Assert.assertEquals("method .fieldIndex", 1, fields.fieldIndex("bar"));
        try {
            fields.fieldIndex("baz");
            Assert.fail("method .fieldIndex Expected Exception not Thrown");
        } catch (IllegalArgumentException e) {
        }

        //testing contains
        Assert.assertEquals("method .contains", true, fields.contains("foo"));
        Assert.assertEquals("method .contains", true, fields.contains("bar"));
        Assert.assertEquals("method .contains", false, fields.contains("baz"));

        //testing toList
        Assert.assertTrue("method .toList", fields.toList() instanceof List);
        Assert.assertEquals("method .toList", 2, fields.toList().size());
        Assert.assertEquals("method .toList", Lists.newArrayList("foo", "bar"), fields.toList());

        //testing iterator
        Assert.assertTrue("method .iterator", fields.iterator() instanceof Iterator);
        Assert.assertEquals("method .iterator", 2, Lists.newArrayList(fields.iterator()).size());
        Assert.assertEquals("method .iterator", Lists.newArrayList("foo", "bar"), Lists.newArrayList(fields.iterator()));

        //testing select
        List selected = fields.select(new Fields("bar"), Lists.<Object>newArrayList("a", "b", "c"));
        Assert.assertTrue("method .select", selected instanceof List);
        Assert.assertEquals("method .select", Lists.newArrayList("b"), selected);
    }
}