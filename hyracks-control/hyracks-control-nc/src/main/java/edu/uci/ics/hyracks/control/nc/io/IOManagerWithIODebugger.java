/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.control.nc.io;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.FileHandle;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;

public class IOManagerWithIODebugger extends IOManager {

    private int nextFileID;

    private int readIOSeq, readIORand, readIOSeqCrossDev, readIORandCrossDev, writeIOSeq, writeIORand,
            writeIOSeqCrossDev, writeIORandCrossDev;

    /**
     * cache the previous I/O target (file ID) for each device. initialized
     * as -1, meaning the first I/O is always random.
     */
    private List<Integer> fileIDCache;

    /**
     * cache the previous I/O device. Initialized as -1, meaning the first
     * I/O is always cross device.
     */
    private int didCache;

    private int workAreaDeviceIndex, workAreaDeviceCount;

    private static final Logger LOGGER = Logger.getLogger(IOManagerWithIODebugger.class.getSimpleName());

    public IOManagerWithIODebugger(List<IODeviceHandle> devices, Executor executor) throws HyracksException {
        super(devices, executor);

        this.workAreaDeviceIndex = 0;

        this.fileIDCache = new ArrayList<Integer>();

        for (IODeviceHandle d : devices) {
            fileIDCache.add(-1);
            if (d.getWorkAreaPath() != null) {
                workAreaDeviceCount++;
            }
        }

        resetDebugInfo();
    }

    public void resetDebugInfo() {
        LOGGER.warning("IOManager-Reset\t" + getIOStat());

        readIOSeq = 0;
        readIORand = 0;
        readIOSeqCrossDev = 0;
        readIORandCrossDev = 0;
        writeIOSeq = 0;
        writeIORand = 0;
        writeIOSeqCrossDev = 0;
        writeIORandCrossDev = 0;

        this.nextFileID = 0;
        this.didCache = -1;

        for (int i = 0; i < fileIDCache.size(); i++) {
            fileIDCache.set(i, -1);
        }
    }

    @Override
    public int syncWrite(FileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException {

        long timer = System.currentTimeMillis();
        int rtn = super.syncWrite(fHandle, offset, data);
        timer = System.currentTimeMillis() - timer;

        int did = fHandle.getFileReference().getDeviceID();
        int fid = fHandle.getFileReference().getFileID();
        if (did < 0) {
            writeIOSeq++;
        } else {
            if (did != didCache) {
                // cross device
                if (fid != fileIDCache.get(did)) {
                    // random
                    writeIORandCrossDev++;
                    fileIDCache.set(did, fid);
                } else {
                    writeIOSeqCrossDev++;
                }
                didCache = did;
            } else {
                // same device
                if (fid != fileIDCache.get(did)) {
                    // random
                    writeIORand++;
                    fileIDCache.set(did, fid);
                } else {
                    writeIOSeq++;
                }
            }
        }

        return rtn;
    }

    @Override
    public int syncRead(FileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException {

        long timer = System.currentTimeMillis();
        int rtn = super.syncRead(fHandle, offset, data);
        timer = System.currentTimeMillis() - timer;

        int did = fHandle.getFileReference().getDeviceID();
        int fid = fHandle.getFileReference().getFileID();
        if (did < 0) {
            readIOSeq++;
        } else {
            if (did != didCache) {
                // cross device
                if (fid != fileIDCache.get(did)) {
                    // random
                    readIORandCrossDev++;
                    fileIDCache.set(did, fid);
                } else {
                    readIOSeqCrossDev++;
                }
                didCache = did;
            } else {
                // same device
                if (fid != fileIDCache.get(did)) {
                    // random
                    readIORand++;
                    fileIDCache.set(did, fid);
                } else {
                    readIOSeq++;
                }
            }
        }

        return rtn;
    }

    @Override
    public synchronized FileReference createWorkspaceFile(String prefix) throws HyracksDataException {
        FileReference fRef = super.createWorkspaceFile(prefix);

        fRef.setDeviceID(workAreaDeviceIndex);
        fRef.setFileID(nextFileID);

        workAreaDeviceIndex = (workAreaDeviceIndex + 1) % workAreaDeviceCount;
        nextFileID++;

        return fRef;
    }

    /**
     * Log the IO operation.
     * 
     * @param did
     * @param fid
     * @param sameDev
     *            same device? 0 - no, 1 - yes
     * @param rwType
     *            read or write? 0 - read, 1 - write
     * @param srType
     *            seq or rand? 0 - seq, 1 - rand
     */
    private void logIOOperation(long time, int did, int fid, int sameDev, int rwType, int srType) {
        LOGGER.warning("IORecord\t" + time + "\t" + rwType + "\t" + srType + "\t" + sameDev + "\t" + did + "\t" + fid);
    }

    public String getIOStat() {
        return readIOSeq + "\t" + readIORand + "\t" + readIOSeqCrossDev + "\t" + readIORandCrossDev + "\t" + writeIOSeq
                + "\t" + writeIORand + "\t" + writeIOSeqCrossDev + "\t" + writeIORandCrossDev + "\nSimplifiedIO\t"
                + readIOSeq + "\t" + (readIORand + readIORandCrossDev + readIOSeqCrossDev) + "\t" + writeIOSeq + "\t"
                + (writeIORand + writeIORandCrossDev + writeIOSeqCrossDev);
    }

    public String toString() {
        return getIOStat();
    }
}
