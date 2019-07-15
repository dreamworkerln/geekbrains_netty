package ru.geekbrains.netty.selector03.common.entities;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

public class DirectoryReader implements Function<String,String> {

    @Override
    public String apply(String dir) {

        String result = null;

        try {
            StringBuilder sb = new StringBuilder();
            Path path = Paths.get(dir);
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                for (Path entry : stream) {
                    sb.append(entry.getFileName()).append("\n");
                }
            }
            result = sb.toString().trim();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }
}
