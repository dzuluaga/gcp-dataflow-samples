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
import com.google.cloud.dataflow.examples.MinimalWordCount;
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
import java.util.List;
import java.util.Set;


/**
 * An example that counts words in Shakespeare and includes Dataflow best practices.
 *
 * <p>This class, {@link WordCountSmallestTransform}, is the second in a series of four successively more detailed
 * 'word count' examples. You may first want to take a look at {@link MinimalWordCount}.
 * After you've looked at this example, then see the {@link DebuggingWordCount}
 * pipeline, for introduction of additional concepts.
 *
 * <p>For a detailed walkthrough of this example, see
 *   <a href="https://cloud.google.com/dataflow/java-sdk/wordcount-example">
 *   https://cloud.google.com/dataflow/java-sdk/wordcount-example
 *   </a>
 *
 * <p>Basic concepts, also in the MinimalWordCount example:
 * Reading text files; counting a PCollection; writing to GCS.
 *
 * <p>New Concepts:
 * <pre>
 *   1. Executing a Pipeline both locally and using the Dataflow service
 *   2. Using ParDo with static DoFns defined out-of-line
 *   3. Building a composite transform
 *   4. Defining your own pipeline options
 * </pre>
 *
 * <p>Concept #1: you can execute this pipeline either locally or using the Dataflow service.
 * These are now command-line options and not hard-coded as they were in the MinimalWordCount
 * example.
 * To execute this pipeline locally, specify general pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 * }
 * </pre>
 * and a local output file or output prefix on GCS:
 * <pre>{@code
 *   --output=[YOUR_LOCAL_FILE | gs://YOUR_OUTPUT_PREFIX]
 * }</pre>
 *
 * <p>To execute this pipeline using the Dataflow service, specify pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --stagingLocation=gs://YOUR_STAGING_DIRECTORY
 *   --runner=BlockingDataflowPipelineRunner
 * }
 * </pre>
 * and an output prefix on GCS:
 * <pre>{@code
 *   --output=gs://YOUR_OUTPUT_PREFIX
 * }</pre>
 *
 * <p>The input file defaults to {@code gs://dataflow-samples/shakespeare/kinglear.txt} and can be
 * overridden with {@code --inputFile}.
 */
public class WordCountSmallestTransform {

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
  static public class WordCount implements Serializable, Comparable<WordCount>{
    public String name;
    public Long qty;
    public WordCount(String name, Long qty) {
      this.name = name;
      this.qty = qty;
    }

    @Override
    public int compareTo(WordCount wc) {
      return (new Long(this.qty - wc.qty)).intValue();
    }
  }

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

  /** A DoFn that converts a Word and Count into a printable string. */
  public static class FormatAsTextFn2 extends DoFn<String, String> {
    @Override
    public void processElement(ProcessContext c) {
      System.out.println(c.element() + ": " + "1");
      c.output(c.element() + ": " + "1");
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

  public static class SmallestFilter extends PTransform<PCollection<WordCount>,
          PCollection<List<WordCount>>> {
    @Override
    public PCollection<List<WordCount>> apply(PCollection<WordCount> words) {

      PCollection<List<WordCount>> largestValues = words.apply(Top.<WordCount>smallest(10));

      return largestValues;
    }
  }

  /**
   * Options supported by {@link WordCountSmallestTransform}.
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
     .apply(new CountWords2())
     .apply(ParDo.of(new getWordsComparableFn()))
     .apply(new SmallestFilter())
     .apply(ParDo.of(new FormatComparableAsTextFn()))
     .apply(TextIO.Write.named("WriteCounts").to(options.getOutput()));
    p.run();
  }

  /** A DoFn that converts a Word and Count into a printable string. */
  public static class getWordsComparableFn extends DoFn<KV<String, Long>, WordCount> {
    @Override
    public void processElement(ProcessContext c) {
      c.output(
              new WordCount(c.element().getKey(), c.element().getValue()));
    }
  }
}
