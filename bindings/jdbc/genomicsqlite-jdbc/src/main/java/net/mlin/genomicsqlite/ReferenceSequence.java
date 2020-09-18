package net.mlin.genomicsqlite;

public class ReferenceSequence {
  public final long rid, length;
  public final String name, assembly, refgetId, metaJson;

  ReferenceSequence(
      long rid, String name, long length, String assembly, String refgetId, String metaJson) {
    this.rid = rid;
    this.name = name;
    this.length = length;
    this.assembly = assembly;
    this.refgetId = refgetId;
    this.metaJson = metaJson;
  }
}
