package com.example.kafka.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.Arrays;

@Service
public class ProfileService {
  private final Environment environment;

  @Autowired
  public ProfileService(Environment environment) {
    this.environment = environment;
  }

  public boolean match(String profile) {
    return Arrays.stream(environment.getActiveProfiles())
      .anyMatch(profile::equalsIgnoreCase);
  }
}
