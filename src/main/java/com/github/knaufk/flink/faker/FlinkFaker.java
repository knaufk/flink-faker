package com.github.knaufk.flink.faker;

import com.github.javafaker.Faker;
import com.github.javafaker.service.FakeValuesService;
import com.github.javafaker.service.RandomService;
import java.util.Locale;
import java.util.Random;

public class FlinkFaker extends Faker {
  private DateTime dateTime;

  public FlinkFaker() {
    super();
    dateTime = new DateTime(this);
  }

  public FlinkFaker(Locale locale) {
    super(locale);
    dateTime = new DateTime(this);
  }

  public FlinkFaker(Random random) {
    super(random);
    dateTime = new DateTime(this);
  }

  public FlinkFaker(Locale locale, Random random) {
    super(locale, random);
    dateTime = new DateTime(this);
  }

  public FlinkFaker(Locale locale, RandomService randomService) {
    super(locale, randomService);
    dateTime = new DateTime(this);
  }

  public FlinkFaker(FakeValuesService fakeValuesService, RandomService random) {
    super(fakeValuesService, random);
    dateTime = new DateTime(this);
  }

  public DateTime date() {
    return dateTime;
  }
}
