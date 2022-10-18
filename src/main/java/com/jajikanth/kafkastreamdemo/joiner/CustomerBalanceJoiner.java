package com.jajikanth.kafkastreamdemo.joiner;

import com.ibm.gbs.schema.Balance;
import com.ibm.gbs.schema.Customer;
import com.ibm.gbs.schema.CustomerBalance;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class CustomerBalanceJoiner implements ValueJoiner<Balance, Customer, CustomerBalance> {
    @Override
    public CustomerBalance apply(Balance balance, Customer customer) {
        if (customer != null && balance != null && balance.getAccountId() == customer.getAccountId()) {
            return CustomerBalance.newBuilder()
                    .setBalance(balance.getBalance())
                    .setCustomerId(customer.getCustomerId())
                    .setAccountId(balance.getAccountId())
                    .setPhoneNumber(customer.getPhoneNumber())
                    .build();

        } else
            return null;
    }
}
