/**
 * Copyright 2016 Telefonica Investigación y Desarrollo, S.A.U
 *
 * This file is part of fiware-cygnus (FI-WARE project).
 *
 * fiware-cygnus is free software: you can redistribute it and/or modify it under the terms of the GNU Affero
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * fiware-cygnus is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with fiware-cygnus. If not, see
 * http://www.gnu.org/licenses/.
 *
 * For those usages not covered by the GNU Affero General Public License please contact with iot_support at tid dot es
 */
package com.telefonica.iot.cygnus.containers;

import java.util.ArrayList;

/**
 *
 * @author frb
 */
public class AttributeMappings {
    
    private ArrayList<AttributeMapping> attribute_mappings;
    
    /**
     * Constructor.
     */
    public AttributeMappings() {
    } // AttributeMappings
    
    /**
     * Gets a mapping.
     * @param entityId
     * @param entityType
     * @param attrName
     * @param attrType
     * @return A mapping
     */
    public Mapping getMapping(String entityId, String entityType, String attrName, String attrType) {
        for (AttributeMapping attrMap : attribute_mappings) {
            Mapping mapping = attrMap.getMapping(entityId, entityType, attrName, attrType);
            
            if (mapping != null) {
                return mapping;
            } // if
        } // for
        
        return null;
    } // getAttributeMappings
    
    /**
     * Purges any invalid mapping.
     */
    public void purge() {
        ArrayList<AttributeMapping> invalids = new ArrayList<AttributeMapping>();
        
        for (AttributeMapping attributeMapping : attribute_mappings) {
            if (!attributeMapping.isValid()) {
                invalids.add(attributeMapping);
            } // if
        } // for
        
        for (AttributeMapping invalid : invalids) {
            attribute_mappings.remove(invalid);
        } // for
    } // purge
    
    /**
     * Gets if the attribute mappings are empty.
     * @return True if empty, otherwise false
     */
    public boolean isEmpty() {
        return attribute_mappings.isEmpty();
    } // isEmpty

    /**
     * Attribute Mapping class.
     */
    public class AttributeMapping {
        
        private String entity_id;
        private String entity_type;
        ArrayList<Mapping> mappings;
        
        /**
         * Constructor.
         */
        public AttributeMapping() {
        } // AttributeMapping
        
        /**
         * Gets a mapping.
         * @param entityId
         * @param entityType
         * @param attrName
         * @param attrType
         * @return A mapping
         */
        public Mapping getMapping(String entityId, String entityType, String attrName, String attrType) {
            if (entity_id.equals(entityId) && entity_type.equals(entityType)) {
                for (Mapping mapping : mappings) {
                    if (mapping.attr_name.equals(attrName) && mapping.attr_type.equals(attrType)) {
                        return mapping;
                    } // if
                } // for

                return null;
            } else {
                return null;
            } // if else
        } // getMapping
        
        public boolean isValid() {
            if (entity_id == null) {
                return false;
            } // if
            
            if (entity_type == null) {
                return false;
            } // if
            
            for (Mapping mapping : mappings) {
                if (!mapping.isValid()) {
                    return false;
                } // if
            } // for
            
            return true;
        } // isValid
        
    } // AttributeMapping
    
    /**
     * Mapping class.
     */
    public class Mapping {
        private String attr_name;
        private String attr_type;
        private String new_attr_name;
        private String new_attr_type;
        
        /**
         * Constructor.
         */
        public Mapping() {
        } // Mapping
        
        public String getAttrName() {
            return attr_name;
        } // getAttrName
        
        public String getAttrType() {
            return attr_type;
        } // getAttrType
        
        public String getNewAttrName() {
            return new_attr_name;
        } // getNewAttrName
        
        public String getNewAttrType() {
            return new_attr_type;
        } // getNewAttrType
        
        public boolean isValid() {
            if (attr_name == null) {
                return false;
            } // if
            
            if (attr_type == null) {
                return false;
            } // if
            
            if (new_attr_name == null) {
                return false;
            } // if
            
            if (new_attr_type == null) {
                return false;
            } // if
            
            return true;
        } // isValid
        
    } // Mapping
    
} // AttributeMappings
