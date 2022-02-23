package com.github.knaufk.flink.faker;

import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import net.datafaker.DateAndTime;
import net.datafaker.Faker;

public class DateTime extends DateAndTime {

  protected DateTime(Faker faker) {
    super(faker);
  }

  public Timestamp past(int atMost, TimeUnit unit) {
    return new Timestamp(super.past(atMost, unit).getTime());
  }

  public Timestamp past(int atMost, int minimum, TimeUnit unit) {
    return new Timestamp(super.past(atMost, minimum, unit).getTime());
  }

  @Override
  public Timestamp future(int atMost, TimeUnit unit) {
    return new Timestamp(super.future(atMost, unit).getTime());
  }

  @Override
  public Timestamp future(int atMost, int minimum, TimeUnit unit) {
    return new Timestamp(super.future(atMost, minimum, unit).getTime());
  }

  @Override
  public Timestamp future(int atMost, TimeUnit unit, Date referenceDate) {
    return new Timestamp(super.future(atMost, unit, referenceDate).getTime());
  }

  @Override
  public Timestamp past(int atMost, TimeUnit unit, Date referenceDate) {
    return new Timestamp(super.past(atMost, unit, referenceDate).getTime());
  }

  @Override
  public Timestamp between(Date from, Date to) throws IllegalArgumentException {
    return new Timestamp(super.between(from, to).getTime());
  }

  @Override
  public Timestamp birthday() {
    return new Timestamp(super.birthday().getTime());
  }

  @Override
  public Timestamp birthday(int minAge, int maxAge) {
    return new Timestamp(super.birthday(minAge, maxAge).getTime());
  }
}
