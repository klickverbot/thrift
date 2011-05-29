module common;

import std.stdio;

import thrift.test.ThriftTest_types;

void writeInsanityReturn(in Insanity[Numberz][UserId] insane) {
  write("{");
  foreach(key1, value1; insane) {
    writef("%s => {", key1);
    foreach(key2, value2; value1) {
      writef("%s => {", key2);
      write("{");
      foreach(key3, value3; value2.userMap) {
        writef("%s => %s, ", key3, value3);
      }
      write("}, ");

      write("{");
      foreach (x; value2.xtructs) {
        writef("{\"%s\", %s, %s, %s}, ",
          x.string_thing, x.byte_thing, x.i32_thing, x.i64_thing);
      }
      write("}");

      write("}, ");
    }
    write("}, ");
  }
  write("}");
}
