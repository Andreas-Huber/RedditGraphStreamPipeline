package no.simula.umod.redditgraph;

import com.opencsv.CSVReader;
import com.google.common.io.Files;
import com.opencsv.CSVWriter;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class FileUtils {

    private static Reader getFileReaderBasedOnType(java.io.File file) throws IOException, CompressorException {
        var extension = Files.getFileExtension(file.toString());
        if(extension.equals("zst") ){
            var fileInputStream = new FileInputStream(file);
            var bufferedInputStream = new BufferedInputStream(fileInputStream);
            var compressionName = CompressorStreamFactory.detect(bufferedInputStream);
            var compressorInputStream = new CompressorStreamFactory()
                    .createCompressorInputStream(compressionName, bufferedInputStream, true);
            return new InputStreamReader(compressorInputStream);
        } else {
            return new FileReader(file);
        }
    }

    public static Iterable<String[]> readCsv(java.io.File file) throws IOException, CompressorException {
        Reader reader = getFileReaderBasedOnType(file);
        return new CSVReader(reader);
    }

    public static CompletionStage<Void> exportCsv(Iterable<? extends ToCsv> entities, File outFile, String[] header) {
        return CompletableFuture.runAsync(() -> {
            try {
                final var writer = FileUtils.createCsvWriter(outFile);
                writer.writeNext(header, false);

                for (var edge : entities) {
                    writer.writeNext(edge.toCsvLine(), false);
                }
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private static CSVWriter createCsvWriter(java.io.File file) throws IOException {
        final var fileWriter = new FileWriter(file);
        final var bufferedWriter = new BufferedWriter(fileWriter);
        final var csvWriter = new CSVWriter(bufferedWriter);
        return csvWriter;
    }
}
