-- Separate database for the automated test suite so tests can truncate freely
-- without touching data produced by the demo script.
CREATE DATABASE durable_test OWNER durable;
