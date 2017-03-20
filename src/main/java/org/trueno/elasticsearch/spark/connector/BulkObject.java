/**
 * BulkObject.java
 * This file provides an object for the bulk operation in ES
 *
 * @version 0.0.0.1
 * @author  Victor
 * @modified  maverick-zhn(Servio Palacios)
 * @updated 2017.03.17
 *
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * Do NOT forget to reference the ORIGINAL author of the code.
 */

package org.trueno.elasticsearch.spark.connector;

public class BulkObject {

    private String index;
    private String[][] operations;

    public String[][] getOperations() {
        return operations;
    }

    public void setOperations(String[][] operations) {
        this.operations = operations;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }
}//BulkObject
