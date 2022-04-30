from hummingbot.connector.in_flight_order_base cimport InFlightOrderBase

cdef class HuobiPerpetualInFlightOrder(InFlightOrderBase):
    cdef:
        object trade_id_set
