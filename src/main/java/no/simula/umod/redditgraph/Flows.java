package no.simula.umod.redditgraph;

import akka.stream.IOResult;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamConverters;
import akka.util.ByteString;
import com.google.common.io.Files;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.*;
import java.util.concurrent.CompletionStage;

public class Flows {

    /**
     * Wraps a Java Compressor Stream into a Source for the given file or returns a file source without compression.
     */
    public static Source<ByteString, CompletionStage<IOResult>> getFileSource(File file) throws IOException, CompressorException {
        final var extension = Files.getFileExtension(file.toString());
        if(extension.equals("zst") || extension.equals("xz") || extension.equals("gz") || extension.equals("bz2")){
            final var fileInputStream = new FileInputStream(file);
            final var bufferedInputStream = new BufferedInputStream(fileInputStream);
            final var compressionName = CompressorStreamFactory.detect(bufferedInputStream);
            final var compressorInputStream = new CompressorStreamFactory()
                            .createCompressorInputStream(compressionName, bufferedInputStream, true);


            return StreamConverters.fromInputStream(() -> compressorInputStream);
        } else {
            return FileIO.fromFile(file);
        }
    }

    /**
     * Wraps a Java Compressor Stream into a Sink for the given file or returns a file sink without compression.
     */
    public static Sink<ByteString, CompletionStage<IOResult>> getFileSink(File file) throws IOException, CompressorException {
        final var extension = Files.getFileExtension(file.toString());
        if(extension.equals("zst")){
            final var fileOutputStream = new FileOutputStream(file);
            final var bufferedOutputStream = new BufferedOutputStream(fileOutputStream);

            final var compressorOutputStream = new CompressorStreamFactory()
                    .createCompressorOutputStream(CompressorStreamFactory.ZSTANDARD, bufferedOutputStream);


            return StreamConverters.fromOutputStream(() -> compressorOutputStream);
        } else {
            return FileIO.toFile(file);
        }
    }
}
