package no.simula.umod.redditgraph;

import com.opencsv.CSVReader;
import com.google.common.io.Files;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.*;

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

    public static Iterable<String[]> readAll(java.io.File file) throws IOException, CompressorException {
        Reader reader = getFileReaderBasedOnType(file);
        return new CSVReader(reader);
    }
}
