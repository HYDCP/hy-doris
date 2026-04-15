// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.httpv2.controller;

import org.apache.doris.common.Config;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Semaphore;

@RestController
@RequestMapping("/rest/v1")
public class LogFilesController {
    private static final Semaphore DOWNLOAD_SEMAPHORE = new Semaphore(1);
    private static final int DEFAULT_MAX_ENTRIES = 2000;
    private static final int DEFAULT_TAIL_BYTES = 1024 * 1024;
    private static final int MAX_TAIL_BYTES = 4 * 1024 * 1024;
    private static final int MIN_TAIL_BYTES = 64 * 1024;

    @RequestMapping(path = "/log_files", method = RequestMethod.GET)
    public Object list(@RequestParam(value = "path", required = false) String path,
                       @RequestParam(value = "include_dir", required = false, defaultValue = "true") boolean includeDir,
                       @RequestParam(value = "include_file", required = false, defaultValue = "true") boolean includeFile,
                       @RequestParam(value = "max_entries", required = false) Integer maxEntries) {
        int limit = maxEntries == null ? DEFAULT_MAX_ENTRIES : Math.max(1, maxEntries);
        Path baseDir;
        Path dir;
        try {
            baseDir = getBaseLogDir();
            dir = resolveInBase(baseDir, path);
        } catch (RuntimeException e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
        if (!Files.exists(dir) || !Files.isDirectory(dir)) {
            return ResponseEntityBuilder.okWithCommonError("path does not exist or is not a directory: " + path);
        }

        Map<String, Object> result = new HashMap<>();
        List<String> columnNames = new ArrayList<>();
        columnNames.add("Name");
        columnNames.add("Size");
        columnNames.add("Last Modified");
        columnNames.add("Type");
        result.put("column_names", columnNames);

        List<Map<String, Object>> rows = new ArrayList<>();
        boolean truncated = false;
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path p : ds) {
                if (rows.size() >= limit) {
                    truncated = true;
                    break;
                }
                BasicFileAttributes attrs;
                try {
                    attrs = Files.readAttributes(p, BasicFileAttributes.class);
                } catch (IOException e) {
                    continue;
                }
                boolean isDir = attrs.isDirectory();
                if (isDir && !includeDir) {
                    continue;
                }
                if (!isDir && !includeFile) {
                    continue;
                }
                String name = p.getFileName().toString();
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("Name", name);
                row.put("Size", isDir ? 0 : attrs.size());
                row.put("Last Modified", attrs.lastModifiedTime().toMillis());
                row.put("Type", isDir ? "DIR" : "FILE");
                String rel = "/" + baseDir.relativize(p).toString().replace('\\', '/');
                row.put("__path", rel);
                rows.add(row);
            }
        } catch (IOException e) {
            return ResponseEntityBuilder.okWithCommonError("failed to list directory: " + e.getMessage());
        }
        result.put("rows", rows);
        result.put("truncated", truncated);
        result.put("base_log_dir", baseDir.toString());
        result.put("path", normalizePath(path));
        return ResponseEntityBuilder.ok(result);
    }

    @RequestMapping(path = "/log_file/view", method = RequestMethod.GET)
    public Object view(@RequestParam(value = "path") String path,
                       @RequestParam(value = "tail_bytes", required = false) Integer tailBytes) {
        int n = tailBytes == null ? DEFAULT_TAIL_BYTES : tailBytes;
        n = Math.min(MAX_TAIL_BYTES, Math.max(MIN_TAIL_BYTES, n));
        Path baseDir;
        Path file;
        try {
            baseDir = getBaseLogDir();
            file = resolveInBase(baseDir, path);
        } catch (RuntimeException e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
        if (!Files.exists(file) || Files.isDirectory(file)) {
            return ResponseEntityBuilder.okWithCommonError("path does not exist or is not a file: " + path);
        }
        try {
            String content = readTail(file, n);
            return ResponseEntityBuilder.ok(content);
        } catch (IOException e) {
            return ResponseEntityBuilder.okWithCommonError("failed to read file: " + e.getMessage());
        }
    }

    @RequestMapping(path = "/log_file/download", method = RequestMethod.GET)
    public void download(@RequestParam(value = "path") String path,
                         @RequestParam(value = "download_name", required = false) String downloadName,
                         HttpServletRequest request, HttpServletResponse response) throws IOException {
        if (!DOWNLOAD_SEMAPHORE.tryAcquire()) {
            response.sendError(429, "another download is in progress");
            return;
        }
        try {
            Path baseDir;
            Path file;
            try {
                baseDir = getBaseLogDir();
                file = resolveInBase(baseDir, path);
            } catch (RuntimeException e) {
                response.sendError(400, e.getMessage());
                return;
            }
            if (!Files.exists(file) || Files.isDirectory(file)) {
                response.sendError(404, "path does not exist or is not a file");
                return;
            }
            String name = Strings.isNullOrEmpty(downloadName) ? file.getFileName().toString() : sanitizeFileName(downloadName);
            streamFileWithLimit(file, name, response);
        } finally {
            DOWNLOAD_SEMAPHORE.release();
        }
    }

    @RequestMapping(path = "/log_file/archive", method = RequestMethod.POST)
    public void archive(@RequestBody ArchiveRequest archiveRequest,
                        HttpServletRequest request, HttpServletResponse response) throws IOException {
        if (archiveRequest == null) {
            response.sendError(400, "invalid request");
            return;
        }
        archiveInternal(archiveRequest.paths, archiveRequest.download_name, response);
    }

    @RequestMapping(path = "/log_file/archive", method = RequestMethod.POST, consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
    public void archiveForm(@RequestParam(value = "path") List<String> paths,
                            @RequestParam(value = "download_name", required = false) String downloadName,
                            HttpServletResponse response) throws IOException {
        archiveInternal(paths, downloadName, response);
    }

    private void archiveInternal(List<String> paths, String downloadName, HttpServletResponse response) throws IOException {
        if (paths == null || paths.isEmpty()) {
            response.sendError(400, "paths is empty");
            return;
        }
        if (!DOWNLOAD_SEMAPHORE.tryAcquire()) {
            response.sendError(429, "another download is in progress");
            return;
        }
        try {
            Path baseDir;
            try {
                baseDir = getBaseLogDir();
            } catch (RuntimeException e) {
                response.sendError(500, e.getMessage());
                return;
            }
            List<Path> files = new ArrayList<>();
            for (String p : paths) {
                Path file;
                try {
                    file = resolveInBase(baseDir, p);
                } catch (RuntimeException e) {
                    response.sendError(400, e.getMessage());
                    return;
                }
                if (!Files.exists(file) || Files.isDirectory(file)) {
                    response.sendError(404, "path does not exist or is not a file: " + p);
                    return;
                }
                files.add(file);
            }

            String outName = Strings.isNullOrEmpty(downloadName)
                    ? "fe_logs.tar.gz"
                    : sanitizeFileName(downloadName);
            if (!outName.toLowerCase(Locale.ROOT).endsWith(".tar.gz")) {
                outName = outName + ".tar.gz";
            }

            response.setContentType("application/gzip");
            response.setHeader("Content-Disposition", "attachment; filename=" + encodeAttachmentFilename(outName));

            RateLimiter limiter = createRateLimiter();
            try (OutputStream os = response.getOutputStream();
                 GzipCompressorOutputStream gzip = new GzipCompressorOutputStream(os);
                 TarArchiveOutputStream tar = new TarArchiveOutputStream(gzip)) {
                tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
                byte[] buf = new byte[64 * 1024];
                for (Path f : files) {
                    String entryName = baseDir.relativize(f).toString().replace('\\', '/');
                    if (Strings.isNullOrEmpty(entryName)) {
                        entryName = f.getFileName().toString();
                    }
                    TarArchiveEntry entry = new TarArchiveEntry(f.toFile(), entryName);
                    tar.putArchiveEntry(entry);
                    try (BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(f))) {
                        int r;
                        while ((r = bis.read(buf)) >= 0) {
                            if (r == 0) {
                                continue;
                            }
                            if (limiter != null) {
                                limiter.acquire(r);
                            }
                            tar.write(buf, 0, r);
                        }
                    }
                    tar.closeArchiveEntry();
                }
                tar.finish();
            }
        } finally {
            DOWNLOAD_SEMAPHORE.release();
        }
    }

    public static class ArchiveRequest {
        public List<String> paths;
        public String format;
        public String download_name;
    }

    private static String readTail(Path file, int tailBytes) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            long fileSize = raf.length();
            long startPos = fileSize < tailBytes ? 0L : fileSize - tailBytes;
            raf.seek(startPos);
            byte[] data = new byte[(int) (fileSize - startPos)];
            raf.readFully(data);
            return new String(data, StandardCharsets.UTF_8);
        }
    }

    private static void streamFileWithLimit(Path file, String downloadName, HttpServletResponse response)
            throws IOException {
        response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
        response.setHeader("Content-Disposition", "attachment; filename=" + encodeAttachmentFilename(downloadName));

        RateLimiter limiter = createRateLimiter();
        byte[] buf = new byte[64 * 1024];
        try (OutputStream os = response.getOutputStream();
             BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(file))) {
            int r;
            while ((r = bis.read(buf)) >= 0) {
                if (r == 0) {
                    continue;
                }
                if (limiter != null) {
                    limiter.acquire(r);
                }
                os.write(buf, 0, r);
            }
        }
    }

    private static RateLimiter createRateLimiter() {
        int kbs = Config.web_log_download_rate_limit_kbs;
        if (kbs <= 0) {
            return null;
        }
        long bytesPerSec = (long) kbs * 1024L;
        return RateLimiter.create((double) bytesPerSec);
    }

    private static Path getBaseLogDir() {
        String base = Strings.isNullOrEmpty(Config.sys_log_dir) ? System.getenv("LOG_DIR") : Config.sys_log_dir;
        if (Strings.isNullOrEmpty(base)) {
            throw new IllegalStateException("LOG_DIR is empty");
        }
        return Paths.get(base).normalize();
    }

    private static Path resolveInBase(Path baseDir, String inputPath) {
        String p = normalizePath(inputPath);
        if (p.startsWith("/")) {
            p = p.substring(1);
        }
        Path resolved = baseDir.resolve(p).normalize();
        if (!resolved.startsWith(baseDir)) {
            throw new IllegalArgumentException("invalid path");
        }
        return resolved;
    }

    private static String normalizePath(String path) {
        if (Strings.isNullOrEmpty(path)) {
            return "/";
        }
        String p = path.trim();
        if (p.isEmpty()) {
            return "/";
        }
        if (!p.startsWith("/")) {
            p = "/" + p;
        }
        return p;
    }

    private static String sanitizeFileName(String name) {
        String n = name.replace('\\', '_').replace('/', '_');
        n = n.replace("\r", "_").replace("\n", "_");
        if (n.isEmpty()) {
            return "download";
        }
        return n;
    }

    private static String encodeAttachmentFilename(String fileName) {
        String encoded = URLEncoder.encode(fileName, StandardCharsets.UTF_8);
        return encoded.replace("+", "%20");
    }
}
