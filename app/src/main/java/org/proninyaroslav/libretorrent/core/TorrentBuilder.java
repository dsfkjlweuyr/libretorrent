/*
 * Copyright (C) 2019-2026 Yaroslav Pronin <proninyaroslav@mail.ru>
 *
 * This file is part of LibreTorrent.
 *
 * LibreTorrent is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * LibreTorrent is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with LibreTorrent.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.proninyaroslav.libretorrent.core;

import android.content.Context;
import android.net.Uri;
import android.text.TextUtils;

import androidx.annotation.NonNull;

import org.proninyaroslav.libretorrent.core.exception.UnknownUriException;
import org.proninyaroslav.libretorrent.core.system.SystemFacadeHelper;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.core.SingleEmitter;
import io.reactivex.rxjava3.functions.Predicate;
import io.reactivex.rxjava3.subjects.BehaviorSubject;

/**
 * Pure-Java BitTorrent v1, v2 (BEP 52), and hybrid torrent file builder.
 */
public class TorrentBuilder {
    private static final int BLOCK_SIZE = 16 * 1024; /* 16 KiB (BEP 52 leaf block size) */
    private static final int MAX_AUTO_PIECE_SIZE = 16 * 1024 * 1024; /* 16 MiB */
    private static final int TARGET_MAX_PIECES = 2000;
    private static final byte[] ZERO_HASH = new byte[32];

    private final Context context;
    private final BehaviorSubject<Progress> progress = BehaviorSubject.create();

    private File seedFile;
    private int pieceSize = 0;
    private TorrentVersion torrentVersion = TorrentVersion.HYBRID;
    private final List<Tracker> trackers = new ArrayList<>();
    private final List<String> urlSeeds = new ArrayList<>();
    private boolean isPrivate = false;
    private String creator;
    private String comment;
    private Predicate<String> fileNameFilter;

    public record Tracker(@NonNull String url, int tier) {
    }

    public record Progress(int piece, int numPieces) {
    }

    public enum TorrentVersion {
        V1_ONLY,
        V2_ONLY,
        HYBRID
    }

    private record SourceFile(File file, List<String> pathSegments, long length) {
    }

    public TorrentBuilder(@NonNull Context context) {
        this.context = context;
    }

    public TorrentBuilder setSeedPath(Uri path) throws UnknownUriException {
        String seedPathStr = SystemFacadeHelper.getFileSystemFacade(context)
                .makeFileSystemPath(path);
        if (TextUtils.isEmpty(seedPathStr)) {
            throw new UnknownUriException("Empty file system path for " + path);
        }
        seedFile = new File(seedPathStr);

        return this;
    }

    /**
     * The size of each piece in bytes. It must be a power of 2 >= 16 KiB.
     * If 0 is specified, a piece size will be calculated automatically.
     */
    public TorrentBuilder setPieceSize(int size) {
        this.pieceSize = Math.max(0, size);

        return this;
    }

    public TorrentBuilder addTrackers(@NonNull List<Tracker> trackers) {
        this.trackers.addAll(trackers);

        return this;
    }

    public TorrentBuilder addUrlSeeds(@NonNull List<String> urls) {
        for (String url : urls) {
            if (!TextUtils.isEmpty(url)) {
                this.urlSeeds.add(url);
            }
        }

        return this;
    }

    public TorrentBuilder setAsPrivate(boolean isPrivate) {
        this.isPrivate = isPrivate;

        return this;
    }

    public TorrentBuilder setCreator(String creator) {
        this.creator = creator;

        return this;
    }

    public TorrentBuilder setComment(String comment) {
        this.comment = comment;

        return this;
    }

    public TorrentBuilder setFileNameFilter(Predicate<String> fileNameFilter) {
        this.fileNameFilter = fileNameFilter;

        return this;
    }

    public TorrentBuilder setTorrentVersion(TorrentVersion version) {
        if (version != null) {
            this.torrentVersion = version;
        }

        return this;
    }

    public Observable<Progress> observeProgress() {
        return progress;
    }

    public Single<byte[]> build() {
        return Single.create((emitter) -> {
            try {
                byte[] bencode = buildInternal(emitter);
                if (!emitter.isDisposed()) {
                    emitter.onSuccess(bencode);
                }
            } catch (Throwable t) {
                if (!emitter.isDisposed()) {
                    emitter.onError(t);
                }
            }
        });
    }

    private byte[] buildInternal(SingleEmitter<byte[]> emitter) throws Exception {
        if (seedFile == null || !seedFile.exists()) {
            throw new FileNotFoundException(seedFile == null
                    ? "Seed path is not set"
                    : seedFile.getAbsolutePath());
        }

        boolean isSingleFile = seedFile.isFile();
        List<SourceFile> files = collectFiles(seedFile, isSingleFile);
        long totalSize = 0;
        for (SourceFile f : files) {
            totalSize += f.length;
        }
        if (files.isEmpty() || totalSize == 0) {
            throw new IOException("content total size can't be 0");
        }

        int resolvedPieceSize = resolvePieceSize(pieceSize, totalSize);
        int totalPieces = computeTotalPieces(files, totalSize, resolvedPieceSize);

        BencodeDict info = new BencodeDict();
        info.put("name", seedFile.getName());
        info.put("piece length", (long) resolvedPieceSize);
        if (isPrivate) {
            info.put("private", 1L);
        }

        BencodeDict pieceLayers = new BencodeDict();
        if (torrentVersion == TorrentVersion.V1_ONLY) {
            buildV1(emitter, info, files, isSingleFile, resolvedPieceSize, totalPieces);
        } else {
            buildV2OrHybrid(
                    emitter,
                    info,
                    pieceLayers,
                    files,
                    isSingleFile,
                    resolvedPieceSize,
                    totalPieces,
                    torrentVersion == TorrentVersion.HYBRID
            );
        }

        if (emitter.isDisposed()) {
            return new byte[0];
        }

        BencodeDict root = new BencodeDict();
        addTrackerFields(root);
        if (!TextUtils.isEmpty(comment)) {
            root.put("comment", comment);
        }
        if (!TextUtils.isEmpty(creator)) {
            root.put("created by", creator);
        }
        root.put("creation date", System.currentTimeMillis() / 1000L);
        if (urlSeeds.size() == 1) {
            root.put("url-list", urlSeeds.get(0));
        } else if (urlSeeds.size() > 1) {
            root.put("url-list", new ArrayList<>(urlSeeds));
        }
        root.put("info", info);
        if (!pieceLayers.isEmpty()) {
            root.put("piece layers", pieceLayers);
        }

        return encodeBencode(root);
    }

    private List<SourceFile> collectFiles(File root, boolean isSingleFile) throws IOException {
        List<SourceFile> result = new ArrayList<>();
        if (isSingleFile) {
            if (acceptFile(root.getName())) {
                result.add(new SourceFile(root, Collections.singletonList(root.getName()), root.length()));
            }
            return result;
        }

        collectDirFiles(root, new ArrayList<>(), result);
        result.sort((a, b) -> compareSegmentLists(a.pathSegments, b.pathSegments));
        return result;
    }

    private void collectDirFiles(
            File dir,
            List<String> prefix,
            List<SourceFile> out
    ) throws IOException {
        File[] children = dir.listFiles();
        if (children == null) {
            throw new IOException("Cannot list directory: " + dir.getAbsolutePath());
        }
        Arrays.sort(children, (a, b) -> compareBytesUnsigned(
                a.getName().getBytes(StandardCharsets.UTF_8),
                b.getName().getBytes(StandardCharsets.UTF_8)
        ));
        for (File child : children) {
            List<String> segments = new ArrayList<>(prefix);
            segments.add(child.getName());
            if (child.isDirectory()) {
                collectDirFiles(child, segments, out);
            } else if (child.isFile()) {
                String relativePath = TextUtils.join("/", segments);
                if (acceptFile(relativePath)) {
                    out.add(new SourceFile(child, segments, child.length()));
                }
            }
        }
    }

    private boolean acceptFile(String relativePath) {
        if (fileNameFilter == null) {
            return true;
        }
        try {
            return fileNameFilter.test(relativePath);
        } catch (Throwable e) {
            return false;
        }
    }

    private static int resolvePieceSize(int requestedPieceSize, long totalSize) {
        if (requestedPieceSize > 0) {
            return Math.max(BLOCK_SIZE, requestedPieceSize);
        }
        int size = BLOCK_SIZE;
        while (totalSize / size > TARGET_MAX_PIECES && size < MAX_AUTO_PIECE_SIZE) {
            size <<= 1;
        }
        return size;
    }

    private int computeTotalPieces(List<SourceFile> files, long totalSize, int pieceSize) {
        if (torrentVersion == TorrentVersion.V1_ONLY) {
            return (int) ((totalSize + pieceSize - 1) / pieceSize);
        }
        long total = 0;
        for (SourceFile file : files) {
            if (file.length > 0) {
                total += (file.length + pieceSize - 1) / pieceSize;
            }
        }
        return (int) Math.max(1, total);
    }

    private void buildV1(
            SingleEmitter<byte[]> emitter,
            BencodeDict info,
            List<SourceFile> files,
            boolean isSingleFile,
            int pieceSize,
            int totalPieces
    ) throws IOException, NoSuchAlgorithmException {
        if (isSingleFile) {
            info.put("length", files.get(0).length);
        } else {
            List<BencodeDict> v1Files = new ArrayList<>(files.size());
            for (SourceFile f : files) {
                BencodeDict fileDict = new BencodeDict();
                fileDict.put("length", f.length);
                fileDict.put("path", new ArrayList<>(f.pathSegments));
                v1Files.add(fileDict);
            }
            info.put("files", v1Files);
        }

        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        ByteArrayOutputStream piecesOut = new ByteArrayOutputStream(totalPieces * 20);
        byte[] buffer = new byte[BLOCK_SIZE];
        int bytesInPiece = 0;
        int pieceIndex = 0;

        for (SourceFile sourceFile : files) {
            if (sourceFile.length == 0) {
                continue;
            }
            try (FileInputStream fis = new FileInputStream(sourceFile.file)) {
                long remainingInFile = sourceFile.length;
                while (remainingInFile > 0) {
                    if (emitter.isDisposed()) {
                        return;
                    }
                    int toRead = (int) Math.min(
                            Math.min(buffer.length, pieceSize - bytesInPiece),
                            remainingInFile
                    );
                    int read = fis.read(buffer, 0, toRead);
                    if (read <= 0) {
                        throw new IOException("Unexpected EOF reading " + sourceFile.file.getAbsolutePath());
                    }
                    sha1.update(buffer, 0, read);
                    bytesInPiece += read;
                    remainingInFile -= read;

                    if (bytesInPiece == pieceSize) {
                        piecesOut.write(sha1.digest());
                        bytesInPiece = 0;
                        pieceIndex++;
                        progress.onNext(new Progress(pieceIndex, totalPieces));
                    }
                }
            }
        }

        if (bytesInPiece > 0) {
            piecesOut.write(sha1.digest());
            pieceIndex++;
            progress.onNext(new Progress(pieceIndex, totalPieces));
        }

        info.put("pieces", piecesOut.toByteArray());
    }

    private void buildV2OrHybrid(
            SingleEmitter<byte[]> emitter,
            BencodeDict info,
            BencodeDict pieceLayers,
            List<SourceFile> files,
            boolean isSingleFile,
            int pieceSize,
            int totalPieces,
            boolean hybrid
    ) throws IOException, NoSuchAlgorithmException {
        info.put("meta version", 2L);

        MessageDigest sha1 = hybrid ? MessageDigest.getInstance("SHA-1") : null;
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        ByteArrayOutputStream v1PiecesOut = hybrid ? new ByteArrayOutputStream(totalPieces * 20) : null;
        List<BencodeDict> v1Files = (hybrid && !isSingleFile) ? new ArrayList<>() : null;

        BencodeDict fileTree = new BencodeDict();
        int blocksPerPiece = Math.max(1, pieceSize / BLOCK_SIZE);
        byte[] buffer = new byte[BLOCK_SIZE];
        byte[] padZeros = hybrid ? new byte[BLOCK_SIZE] : null;
        int hashedPieces = 0;

        for (int i = 0; i < files.size(); i++) {
            if (emitter.isDisposed()) {
                return;
            }
            SourceFile sourceFile = files.get(i);

            if (v1Files != null) {
                BencodeDict fileDict = new BencodeDict();
                fileDict.put("length", sourceFile.length);
                fileDict.put("path", new ArrayList<>(sourceFile.pathSegments));
                v1Files.add(fileDict);
            }

            if (sourceFile.length == 0) {
                addFileToTree(fileTree, sourceFile.pathSegments, 0L, null);
                continue;
            }

            boolean needsPieceLayer = sourceFile.length > pieceSize;
            List<byte[]> filePieceHashes = needsPieceLayer ? new ArrayList<>() : null;
            ByteArrayOutputStream filePieceLayerBytes = needsPieceLayer ? new ByteArrayOutputStream() : null;
            byte[] singlePieceRoot = null;

            try (FileInputStream fis = new FileInputStream(sourceFile.file)) {
                long remainingInFile = sourceFile.length;
                while (remainingInFile > 0) {
                    if (emitter.isDisposed()) {
                        return;
                    }
                    int pieceDataLen = (int) Math.min(pieceSize, remainingInFile);
                    List<byte[]> pieceBlockHashes = new ArrayList<>(blocksPerPiece);
                    int pieceBytesRead = 0;

                    while (pieceBytesRead < pieceDataLen) {
                        if (emitter.isDisposed()) {
                            return;
                        }
                        int blockTarget = Math.min(BLOCK_SIZE, pieceDataLen - pieceBytesRead);
                        int blockRead = 0;
                        while (blockRead < blockTarget) {
                            int n = fis.read(buffer, blockRead, blockTarget - blockRead);
                            if (n <= 0) {
                                throw new IOException("Unexpected EOF reading "
                                        + sourceFile.file.getAbsolutePath());
                            }
                            blockRead += n;
                        }
                        sha256.reset();
                        sha256.update(buffer, 0, blockRead);
                        pieceBlockHashes.add(sha256.digest());

                        if (sha1 != null) {
                            sha1.update(buffer, 0, blockRead);
                        }
                        pieceBytesRead += blockRead;
                    }
                    remainingInFile -= pieceBytesRead;

                    if (sha1 != null) {
                        if (v1Files != null && pieceBytesRead < pieceSize && hasNonEmptyFileAfter(files, i)) {
                            int padLen = pieceSize - pieceBytesRead;
                            int remainingPad = padLen;
                            while (remainingPad > 0) {
                                int chunk = Math.min(padZeros.length, remainingPad);
                                sha1.update(padZeros, 0, chunk);
                                remainingPad -= chunk;
                            }
                            BencodeDict padDict = new BencodeDict();
                            padDict.put("attr", "p");
                            padDict.put("length", (long) padLen);
                            padDict.put("path", Arrays.asList(".pad", String.valueOf(padLen)));
                            v1Files.add(padDict);
                        }
                        v1PiecesOut.write(sha1.digest());
                    }

                    if (needsPieceLayer) {
                        byte[] pieceHash = merkleRoot(sha256, pieceBlockHashes, blocksPerPiece, ZERO_HASH);
                        filePieceHashes.add(pieceHash);
                        filePieceLayerBytes.write(pieceHash);
                    } else {
                        int paddedLeaves = nextPowerOfTwo(pieceBlockHashes.size());
                        singlePieceRoot = merkleRoot(sha256, pieceBlockHashes, paddedLeaves, ZERO_HASH);
                    }

                    hashedPieces++;
                    progress.onNext(new Progress(hashedPieces, totalPieces));
                }
            }

            byte[] piecesRoot;
            if (needsPieceLayer) {
                byte[] piecePadHash = computeUniformSubtreePadHash(sha256, blocksPerPiece);
                int paddedPieces = nextPowerOfTwo(filePieceHashes.size());
                piecesRoot = merkleRoot(sha256, filePieceHashes, paddedPieces, piecePadHash);
                pieceLayers.put(piecesRoot, filePieceLayerBytes.toByteArray());
            } else {
                piecesRoot = singlePieceRoot;
            }

            addFileToTree(fileTree, sourceFile.pathSegments, sourceFile.length, piecesRoot);
        }

        info.put("file tree", fileTree);
        if (hybrid) {
            if (isSingleFile) {
                info.put("length", files.get(0).length);
            } else {
                info.put("files", v1Files);
            }
            info.put("pieces", v1PiecesOut.toByteArray());
        }
    }

    private static boolean hasNonEmptyFileAfter(List<SourceFile> files, int index) {
        for (int j = index + 1; j < files.size(); j++) {
            if (files.get(j).length > 0) {
                return true;
            }
        }
        return false;
    }

    private static void addFileToTree(
            BencodeDict rootTree,
            List<String> segments,
            long length,
            byte[] piecesRoot
    ) {
        BencodeDict current = rootTree;
        for (int i = 0; i < segments.size(); i++) {
            String segment = segments.get(i);
            Object existing = current.get(segment);
            BencodeDict next;
            if (existing instanceof BencodeDict dict) {
                next = dict;
            } else {
                next = new BencodeDict();
                current.put(segment, next);
            }
            current = next;
        }
        BencodeDict leaf = new BencodeDict();
        leaf.put("length", length);
        if (length > 0 && piecesRoot != null) {
            leaf.put("pieces root", piecesRoot);
        }
        current.put("", leaf);
    }

    private static byte[] computeUniformSubtreePadHash(MessageDigest sha256, int leavesCount) {
        byte[] current = ZERO_HASH;
        int n = leavesCount;
        while (n > 1) {
            sha256.reset();
            sha256.update(current);
            sha256.update(current);
            current = sha256.digest();
            n >>= 1;
        }
        return current;
    }

    private static byte[] merkleRoot(
            MessageDigest sha256,
            List<byte[]> nodes,
            int targetPowerOfTwoSize,
            byte[] padHash
    ) {
        if (nodes.isEmpty()) {
            return padHash;
        }
        if (targetPowerOfTwoSize == 1) {
            return nodes.get(0);
        }
        List<byte[]> level = new ArrayList<>(targetPowerOfTwoSize);
        level.addAll(nodes);
        while (level.size() < targetPowerOfTwoSize) {
            level.add(padHash);
        }
        while (level.size() > 1) {
            List<byte[]> next = new ArrayList<>(level.size() / 2);
            for (int i = 0; i < level.size(); i += 2) {
                sha256.reset();
                sha256.update(level.get(i));
                sha256.update(level.get(i + 1));
                next.add(sha256.digest());
            }
            level = next;
        }
        return level.get(0);
    }

    private static int nextPowerOfTwo(int n) {
        if (n <= 1) {
            return 1;
        }
        int highest = Integer.highestOneBit(n);
        return (n == highest) ? n : (highest << 1);
    }

    private void addTrackerFields(BencodeDict root) {
        if (trackers.isEmpty()) {
            return;
        }
        TreeMap<Integer, List<String>> tiers = new TreeMap<>();
        for (Tracker tracker : trackers) {
            tiers.computeIfAbsent(tracker.tier(), (k) -> new ArrayList<>()).add(tracker.url());
        }
        List<List<String>> announceList = new ArrayList<>(tiers.values());
        if (!announceList.isEmpty() && !announceList.get(0).isEmpty()) {
            root.put("announce", announceList.get(0).get(0));
        }
        if (trackers.size() > 1) {
            root.put("announce-list", announceList);
        }
    }

    private static byte[] encodeBencode(Object root) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeBencodeValue(out, root);
        return out.toByteArray();
    }

    private static void writeBencodeValue(ByteArrayOutputStream out, Object value) throws IOException {
        if (value instanceof Long l) {
            out.write('i');
            out.write(Long.toString(l).getBytes(StandardCharsets.US_ASCII));
            out.write('e');
        } else if (value instanceof Integer i) {
            out.write('i');
            out.write(Integer.toString(i).getBytes(StandardCharsets.US_ASCII));
            out.write('e');
        } else if (value instanceof byte[] bytes) {
            out.write(Integer.toString(bytes.length).getBytes(StandardCharsets.US_ASCII));
            out.write(':');
            out.write(bytes);
        } else if (value instanceof String s) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            out.write(Integer.toString(bytes.length).getBytes(StandardCharsets.US_ASCII));
            out.write(':');
            out.write(bytes);
        } else if (value instanceof List<?> list) {
            out.write('l');
            for (Object item : list) {
                writeBencodeValue(out, item);
            }
            out.write('e');
        } else if (value instanceof BencodeDict dict) {
            out.write('d');
            for (Map.Entry<byte[], Object> entry : dict.entrySet()) {
                writeBencodeValue(out, entry.getKey());
                writeBencodeValue(out, entry.getValue());
            }
            out.write('e');
        } else {
            throw new IllegalArgumentException("Unsupported bencode type: "
                    + (value == null ? "null" : value.getClass().getName()));
        }
    }

    private static int compareSegmentLists(List<String> a, List<String> b) {
        int min = Math.min(a.size(), b.size());
        for (int i = 0; i < min; i++) {
            int cmp = compareBytesUnsigned(
                    a.get(i).getBytes(StandardCharsets.UTF_8),
                    b.get(i).getBytes(StandardCharsets.UTF_8)
            );
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(a.size(), b.size());
    }

    private static int compareBytesUnsigned(byte[] a, byte[] b) {
        int min = Math.min(a.length, b.length);
        for (int i = 0; i < min; i++) {
            int ba = a[i] & 0xFF;
            int bb = b[i] & 0xFF;
            if (ba != bb) {
                return Integer.compare(ba, bb);
            }
        }
        return Integer.compare(a.length, b.length);
    }

    private static final class BencodeDict extends TreeMap<byte[], Object> {
        BencodeDict() {
            super(TorrentBuilder::compareBytesUnsigned);
        }

        void put(String key, Object value) {
            super.put(key.getBytes(StandardCharsets.UTF_8), value);
        }

        Object get(String key) {
            return super.get(key.getBytes(StandardCharsets.UTF_8));
        }
    }
}
