// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

package com.yahoo.vespa.config.server.filedistribution;

import com.yahoo.config.FileReference;
import com.yahoo.config.model.api.FileDistribution;
import com.yahoo.io.IOUtils;
import com.yahoo.log.LogLevel;
import com.yahoo.text.Utf8;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.logging.Logger;

public class FileDirectory  {
    private static final Logger log = Logger.getLogger(FileDirectory.class.getName());
    private final File root;

    public FileDirectory() {
        this(FileDistribution.getDefaultFileDBPath());
    }

    public FileDirectory(File rootDir) {
        root = rootDir;
        try {
            ensureRootExist();
        } catch (IllegalArgumentException e) {
            log.log(LogLevel.WARNING, "Failed creating directory in constructor, will retry on demand : " + e.toString());
        }
    }

    private void ensureRootExist() {
        if (! root.exists()) {
            if ( ! root.mkdir()) {
                throw new IllegalArgumentException("Failed creating root dir '" + root.getAbsolutePath() + "'.");
            }
        } else if (!root.isDirectory()) {
            throw new IllegalArgumentException("'" + root.getAbsolutePath() + "' is not a directory");
        }
    }

    static private class Filter implements FilenameFilter {
        @Override
        public boolean accept(File dir, String name) {
            return !".".equals(name) && !"..".equals(name) ;
        }
    }

    String getPath(FileReference ref) {
        return root.getAbsolutePath() + "/" + ref.value();
    }

    File getFile(FileReference reference) {
        ensureRootExist();
        File dir = new File(getPath(reference));
        if (!dir.exists()) {
            throw new IllegalArgumentException("File reference '" + reference.toString() + "' with absolute path '" + dir.getAbsolutePath() + "' does not exist.");
        }
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("File reference '" + reference.toString() + "' with absolute path '" + dir.getAbsolutePath() + "' is not a directory.");
        }
        File [] files = dir.listFiles(new Filter());
        if (files == null || files.length == 0) {
            throw new IllegalArgumentException("File reference '" + reference.toString() + "' with absolute path '" + dir.getAbsolutePath() + " does not contain any files");
        }
        return files[0];
    }

    private Long computeReference(File file) throws IOException {
        byte [] wholeFile = IOUtils.readFileBytes(file);
        XXHash64 hasher = XXHashFactory.fastestInstance().hash64();
        return hasher.hash(ByteBuffer.wrap(wholeFile), hasher.hash(ByteBuffer.wrap(Utf8.toBytes(file.getName())), 0));
    }

    public FileReference addFile(File source) {
        try {
            Long hash = computeReference(source);
            FileReference reference = new FileReference(Long.toHexString(hash));
            return addFile(source, reference);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    FileReference addFile(File source, FileReference reference) {
        ensureRootExist();
        try {
            logfileInfo(source);
            File destinationDir = new File(root, reference.value());
            Path tempDestinationDir = Files.createTempDirectory(root.toPath(), "writing");
            File destination = new File(tempDestinationDir.toFile(), source.getName());
            if (!destinationDir.exists()) {
                destinationDir.mkdir();
                log.log(LogLevel.DEBUG, "file reference ' " + reference.value() + "', source: " + source.getAbsolutePath() );
                if (source.isDirectory()) {
                    log.log(LogLevel.DEBUG, "Copying source " + source.getAbsolutePath() + " to " + destination.getAbsolutePath());
                    IOUtils.copyDirectory(source, destination, -1);
                } else
                    copyFile(source, destination);
                if (!destinationDir.exists()) {
                    log.log(LogLevel.DEBUG, "Moving from " + tempDestinationDir + " to " + destinationDir.getAbsolutePath());
                    if ( ! tempDestinationDir.toFile().renameTo(destinationDir)) {
                        log.log(LogLevel.WARNING, "Failed moving '" + tempDestinationDir.toFile().getAbsolutePath() + "' to '" + destination.getAbsolutePath() + "'.");
                    }
                } else {
                    IOUtils.copyDirectory(tempDestinationDir.toFile(), destinationDir, -1);
                }
            }
            IOUtils.recursiveDeleteDir(tempDestinationDir.toFile());
            return reference;
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private void logfileInfo(File file ) throws IOException {
        BasicFileAttributes basicFileAttributes = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
        log.log(LogLevel.DEBUG, "Adding file " + file.getAbsolutePath() + " (created " + basicFileAttributes.creationTime() +
                ", modified " + basicFileAttributes.lastModifiedTime() +
                ", size " + basicFileAttributes.size() + ")");
    }

    private static void copyFile(File source, File dest) throws IOException {
        try (FileChannel sourceChannel = new FileInputStream(source).getChannel();
             FileChannel destChannel = new FileOutputStream(dest).getChannel()) {
            destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());
        }
    }
}
