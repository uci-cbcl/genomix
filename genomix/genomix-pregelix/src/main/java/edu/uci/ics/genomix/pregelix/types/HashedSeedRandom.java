package edu.uci.ics.genomix.pregelix.types;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import edu.uci.ics.genomix.data.utils.Marshal;

/**
 * A random number generator that hashes its seeds to break any correlation in similar seeds
 */
@SuppressWarnings("serial")
public class HashedSeedRandom extends Random {
    MessageDigest md;

    @Override
    public void setSeed(long seed) {
        if (md == null) {
            try {
                md = MessageDigest.getInstance("SHA1");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        } else {
            md.reset();
        }
        for (int i = 0; i < Long.SIZE; i += Byte.SIZE) {
            md.update((byte) (seed >>> i));
        }
        super.setSeed(Marshal.getLong(md.digest(), 0));
    }
}
