package com.kafka.producer.egypt;

public class Egypt {
    String vaccinations;
    String advise;
    String currency;

    public Egypt(String vaccinations, String advise, String currency){
        this.advise = advise;
        this.currency = currency;
        this.vaccinations = vaccinations;
    }
    public String getVaccinations() {
        return vaccinations;
    }

    public String getAdvise() {
        return advise;
    }

    public String getCurrency() {
        return currency;
    }

    public void setVaccinations(String vaccinations) {
        this.vaccinations = vaccinations;
    }

    public void setAdvise(String advise) {
        this.advise = advise;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }
}
