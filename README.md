gcp-dataflow-examples
=====================

The goal of this repo is to provide more examples of gcp dataflow that help novice users of GCP DataFlow to learn basic concepts.

#### How to execute this examples?

##### Local
```java
mvn compile exec:java \
      -Dexec.mainClass=com.google.cloud.dataflow.examples.dz.WordCountApplyGreaterThanEqFilter \
      -Dexec.args="--output=./output/"
```

##### Custom POJOs: Serializable interface and Comparable interfaces
In order to support Filter and Top transformations on PCollections on POJOs, I had to implement Seriablizable and Comparable interfaces. Also note annotation `@DefaultCoder' annotation.

```java
  @DefaultCoder(SerializableCoder.class)
  static public class WordCount implements Serializable, Comparable<WordCount>{
    public String name;
    public Long qty;
    public WordCount(String name, Long qty) {
      this.name = name;
      this.qty = qty;
    }
```


##### WordCountApplyGreaterThanEqFilter Class
This class leverages `greaterThanEq` static function from Filter Class.


#### WordCountTopLargestTranform Sample


Note how Top largest function applied to a collection will return a List<T>. In this case, a list of WordCount.

```java
      PCollection<List<WordCount>> largestValues = words.apply(Top.<WordCount>largest(10));

  /** A DoFn that converts a Word and Count into a printable string. */
  public static class FormatComparableAsTextFn extends DoFn<List<WordCount>, String> {
    @Override
    public void processElement(ProcessContext c) {
      //c.output(c.element().name + ": " + c.element().qty);  // c.element().name  c.element().qty
      for(WordCount wordCount: c.element()) {
        c.output(wordCount.name + ":" + wordCount.qty);
      }
    }
  }

```

Note how FormatComparableAsTextFn method is used to output the content of List<WordCount>, which is wrapped by ProcessContext object.

#### WordCountSmallestTranform Sample

Returns the smallest x elements in the list.

