/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.examples.dz;

import com.google.cloud.dataflow.examples.DebuggingWordCount;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.coders.SetCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.io.Serializable;
import java.util.Set;


/**
 * An example that counts words in Shakespeare and includes Dataflow best practices.
 *
 */
public class WordCountCustomClass {

  /**
   * Concept #2: You can make your pipeline code less verbose by defining your DoFns statically out-
   * of-line. This DoFn tokenizes lines of text into individual words; we pass it to a ParDo in the
   * pipeline.
   */
  static class ExtractWordsFn extends DoFn<String, String> {
    private final Aggregator<Long, Long> emptyLines =
        createAggregator("emptyLines", new Sum.SumLongFn());

    @Override
    public void processElement(ProcessContext c) {
      if (c.element().trim().isEmpty()) {
        emptyLines.addValue(1L);
      }

      // Split the line into words.
      String[] words = c.element().split("[^a-zA-Z']+");

      // Output each word encountered into the output PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          c.output(word);
        }
      }
    }
  }

  @DefaultCoder(SerializableCoder.class)
  static public class CustomClass implements Serializable{
    public String name;
    public Long qty;
    public CustomClass(String name, Long qty) {
      this.name = name;
      this.qty = qty;
    }
  }

  /** A DoFn that converts a Word and Count into a printable string. */
  public static class FormatAsTextFn extends DoFn<KV<String, Long>, String> {
    @Override
    public void processElement(ProcessContext c) {
      c.output(c.element().getKey() + ": " + c.element().getValue());
    }
  }

  /** A DoFn that converts a Word and Count into a printable string. */
/*  public static class FormatAsTextFnTest extends DoFn<KV<String, Long>, WordCount> {
    @Override
    public void processElement(ProcessContext c) {
      //c.output(c.element() + " TEST ");
      c.output(new WordCount(c.element().getKey(), c.element().getValue()));
    }
  }*/

  /** A DoFn that converts a Word and Count into a printable string. */
  public static class FormatComparableAsTextFn extends DoFn<CustomClass, String> {
    @Override
    public void processElement(ProcessContext c) {
      c.output(c.element().name + ": " + c.element().qty);  // c.element().name  c.element().qty
    }
  }

  /** A DoFn that converts a Word and Count into a printable string. */
  public static class FormatAsTextFn2 extends DoFn<String, String> {
    @Override
    public void processElement(ProcessContext c) {
      System.out.println(c.element() + ": " + "1");
      c.output(c.element() + ": " + "1");
    }
  }

  /**
   * A PTransform that converts a PCollection containing lines of text into a PCollection of
   * formatted word counts.
   *
   * <p>Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
   * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
   * modular testing, and an improved monitoring experience.
   */
  public static class CountWords extends PTransform<PCollection<String>,
      PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> apply(PCollection<String> lines) {

      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(
          ParDo.of(new ExtractWordsFn()));

      // Count the number of times each word occurs.
      PCollection<KV<String, Long>> wordCounts =
          words.apply(Count.<String>perElement());

      return wordCounts;
    }
  }

  public static class CountWords2 extends PTransform<PCollection<String>,
          PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> apply(PCollection<String> words) {

      // Count the number of times each word occurs.
      PCollection<KV<String, Long>> wordCounts =
              words.apply(Count.<String>perElement());

      return wordCounts;
    }
  }

//  public static class FilterElements extends PTransform<PCollection<KV<String, Long>>,
//          PCollection<KV<String, Long>>> {
//    @Override
//    public PCollection<KV<String, Long>> apply(PCollection<KV<String, Long>> words) {
//
//      // Count the number of times each word occurs.
////      PCollection<KV<String, Long>> wordCounts =
////              words.apply(Count.<String>perElement());
//
//      words.apply(Filter.lessThan());
//
//      return wordCounts;
//    }
//  }

/*  public static class CountWords3 extends PTransform<PCollection<String>,
          PCollection<String>> {
    @Override
    public PCollection<String> apply(PCollection<String> lines) {

      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(
              ParDo.of(new ExtractWordsFn()));


      PCollection<String> uselessWords = words.apply(new ExtractWordsFn());
      // Count the number of times each word occurs.
*//*      PCollection<KV<String, Long>> wordCounts =
              //words.apply(Count2.<String>perElement());
              words.apply(Count.<String>perElement());*//*


      //return wordCounts;
      return words;
    }
  }*/


  /**
   * Options supported by {@link WordCountCustomClass}.
   *
   * <p>Concept #4: Defining your own configuration options. Here, you can add your own arguments
   * to be processed by the command-line parser, and specify default values for them. You can then
   * access the options values in your pipeline code.
   *
   * <p>Inherits standard configuration options.
   */
  public interface WordCountOptions extends PipelineOptions {
    @Description("Path of the file to read from")
    @Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
    String getInputFile();
    void setInputFile(String value);

    @Description("Path of the file to write to")
    @Default.InstanceFactory(OutputFactory.class)
    String getOutput();
    void setOutput(String value);

    /**
     * Returns "gs://${YOUR_STAGING_DIRECTORY}/counts.txt" as the default destination.
     */
    class OutputFactory implements DefaultValueFactory<String> {
      @Override
      public String create(PipelineOptions options) {
        DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
        if (dataflowOptions.getStagingLocation() != null) {
          return GcsPath.fromUri(dataflowOptions.getStagingLocation())
              .resolve("counts.txt").toString();
        } else {
          throw new IllegalArgumentException("Must specify --output or --stagingLocation");
        }
      }
    }

  }

  public static void main(String[] args) {
    WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
      .as(WordCountOptions.class);
    Pipeline p = Pipeline.create(options);
    p.getCoderRegistry().registerCoder(Set.class, SetCoder.class);

    // Concepts #2 and #3: Our pipeline applies the composite CountWords transform, and passes the
    // static FormatAsTextFn() to the ParDo transform.
    p.apply(TextIO.Read.named("ReadLines").from(options.getInputFile()))
     //.apply(ParDo.of(new FormatAsTextFn2()))
     .apply(ParDo.of(new ExtractWordsFn()))
     //.apply(new ToUpper())
     .apply(new ToLower())
     .apply(new CountWords2())
     //.apply(ParDo.of(new ExtractWordsFn3()))
     //.apply(ParDo.of(new FormatAsTextFn()))
     .apply(ParDo.of(new getWordsComparableFn()))
/*     .apply(ParDo.of(new DoFn<WordCount, String>() {
       @Override
       public void processElement(ProcessContext c) throws Exception {
         c.output(c.element().name + ":" + c.element().qty);
       }
     }))*/
     //.apply(ParDo.of(new getWordsComparableFn())
     //.apply(ParDo.of(new FormatAsTextFn()))
     .apply(ParDo.of(new FormatComparableAsTextFn()))
     .apply(TextIO.Write.named("WriteCounts").to(options.getOutput()));
    p.run();
  }

  static class ExtractWordsFn3 extends DoFn<String, String>{
    @Override
    public void processElement(ProcessContext c) {
      for (String word : c.element().split("[^a-zA-Z']+")) {
        if (!word.isEmpty()) {
          System.out.println(word);
          if(word.equals("como")) {
            c.output(word);
          }
          //c.output(word);
        }
      }
    }
  }

  public static class ToUpper extends PTransform<PCollection<String>,
          PCollection<String>> {
    @Override
    public PCollection<String> apply(PCollection<String> words) {
      return words.apply(ParDo.of(new DoFn<String, String>() {
        @Override
        public void processElement(ProcessContext c) {
          c.output(c.element().toUpperCase());
        }
      }));
    }
  }

  public static class ToLower extends PTransform<PCollection<String>,
          PCollection<String>> {
    @Override
    public PCollection<String> apply(PCollection<String> words) {
      return words.apply(ParDo.of(new DoFn<String, String>() {
        @Override
        public void processElement(ProcessContext c) {
          c.output(c.element().toLowerCase());
        }
      }));
    }
  }

  /** A DoFn that converts a Word and Count into a printable string. */
  public static class getWordsComparableFn extends DoFn<KV<String, Long>, CustomClass> {
    @Override
    public void processElement(ProcessContext c) {
      c.output(
              new CustomClass(c.element().getKey(), c.element().getValue()));
    }
  }

}
