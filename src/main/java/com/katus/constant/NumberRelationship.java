package com.katus.constant;

import java.io.Serializable;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-18
 */
public enum NumberRelationship implements Serializable {
    EQUAL,
    GREATER_OR_EQUAL,
    GREATER_THAN,
    LESS_OR_EQUAL,
    LESS_THAN,
    NOT_EQUAL;

    public static NumberRelationship getBySymbol(String symbol) {
        switch (symbol) {
            case "=": case "==": case "===": case "eq":
                return EQUAL;
            case ">=": case "ge":
                return GREATER_OR_EQUAL;
            case ">": case "gt":
                return GREATER_THAN;
            case "<=": case "le":
                return LESS_OR_EQUAL;
            case "<": case "lt":
                return LESS_THAN;
            case "!=": case "<>": case "><": case "ne":
                return NOT_EQUAL;
            default:
                return null;
        }
    }

    public boolean check(int compareResult) {
        switch (this) {
            case EQUAL:
                return compareResult == 0;
            case GREATER_OR_EQUAL:
                return compareResult >= 0;
            case GREATER_THAN:
                return compareResult > 0;
            case LESS_OR_EQUAL:
                return compareResult <= 0;
            case LESS_THAN:
                return compareResult < 0;
            case NOT_EQUAL:
                return compareResult != 0;
            default:
                return false;
        }
    }
}
