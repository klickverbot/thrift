module thrift.internal.ctfe;

/*
 * Simple eager join() for strings, std.algorithm.join isn't CTFEable yet.
 */
string ctfeJoin(string[] strings, string separator = ", ") {
  string result;
  if (strings.length > 0) {
    result ~= strings[0];
    foreach (s; strings[1..$]) {
      result ~= separator ~ s;
    }
  }
  return result;
}
