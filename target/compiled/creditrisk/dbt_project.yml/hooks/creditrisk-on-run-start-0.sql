

CREATE SCHEMA IF NOT EXISTS oscreditrisk;

CREATE OR REPLACE FUNCTION oscreditrisk.payment_linearization(
  payment_amounts ARRAY<FLOAT64>,
  daily_rate ARRAY<FLOAT64>,
  reporting_dates ARRAY<STRING>
)

RETURNS ARRAY<FLOAT64>
LANGUAGE js AS """
    let result = [];
    let balance = 0.0;

    for (let i = 0; i < payment_amounts.length; i++) {
        if (payment_amounts[i] > 0) {
            // Add payment to balance
            balance += payment_amounts[i];
        }

        if (balance >= daily_rate[i]) {
            result.push(daily_rate[i]);
            balance = balance - daily_rate[i];
        }
        else if (balance > 0.0) {
            result.push(balance);
            balance = 0.0
        } 
        else {
            // Push zero for days with no payment distribution
            result.push(0.0);
            balance = 0.0
        }
    }
    return result;
""";
  
