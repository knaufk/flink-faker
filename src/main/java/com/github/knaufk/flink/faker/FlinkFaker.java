package com.github.knaufk.flink.faker;

import java.util.Locale;
import java.util.Random;
import net.datafaker.Faker;
import net.datafaker.service.FakeValuesService;
import net.datafaker.service.RandomService;

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
