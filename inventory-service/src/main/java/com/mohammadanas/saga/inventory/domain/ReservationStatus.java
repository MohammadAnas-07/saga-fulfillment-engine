package com.mohammadanas.saga.inventory.domain;

public enum ReservationStatus {

    /** Units are held for this saga. */
    RESERVED,

    /** Units were handed back by the compensating action. Terminal. */
    RELEASED
}
