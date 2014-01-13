package edu.uci.ics.genomix.data.types;

public class ReadHeadSetTest {

    //    @Test
    //    public void TestInitial() {
    //        ReadHeadSet plist = new ReadHeadSet();
    //        Assert.assertEquals(plist.size(), 0);
    //        
    //        byte mateId;
    //        long readId;
    //        int posId;
    //        for (int i = 0; i < 200; i++) {
    //            mateId = (byte)1;
    //            readId = (long)i;
    //            posId = i;
    //            plist.add(mateId, readId, posId);
    //            Assert.assertTrue(plist.contains(new ReadHeadInfo(mateId, readId, posId)));
    //            Assert.assertEquals(i + 1, plist.size());
    //        }
    //        
    //        int i = 0;
    //        for (ReadHeadInfo pos : plist) {
    //            Assert.assertEquals((byte)1, pos.getMateId());
    //            Assert.assertEquals((long) i, pos.getReadId());
    //            Assert.assertEquals(i, pos.getOffset());
    //            i++;
    //        }
    //        
    //    }
    //    
    //    @Test
    //    public void TestRemove() {
    //        ReadHeadSet plist = new ReadHeadSet();
    //        Assert.assertEquals(plist.size(), 0);
    //        
    //        byte mateId;
    //        long readId;
    //        int posId;
    //        for (int i = 0; i < 5; i++) {
    //            mateId = (byte)1;
    //            readId = (long)i;
    //            posId = i;
    //            plist.add(mateId, readId, posId);
    //            Assert.assertTrue(plist.contains(new ReadHeadInfo(mateId, readId, posId)));
    //            Assert.assertEquals(i + 1, plist.size());
    //        }
    //        
    //        int i = 0;
    //        for (ReadHeadInfo pos : plist) {
    //            Assert.assertEquals((byte)1, pos.getMateId());
    //            Assert.assertEquals((long) i, pos.getReadId());
    //            Assert.assertEquals(i, pos.getOffset());
    //            i++;
    //        }
    //        
    //        //delete one element each time
    //        i = 0;
    //        ReadHeadSet copyList = new ReadHeadSet();
    //        copyList.clear();
    //        copyList.addAll(plist);
    //        ReadHeadInfo pos = new ReadHeadInfo(0);
    //        Iterator<ReadHeadInfo> iterator;
    //        for(int j = 0; j < 5; j++){
    //            iterator = copyList.iterator();
    //            ReadHeadInfo deletePos = new ReadHeadInfo(0);
    //            deletePos.set((byte)1, (long)j, j);
    //            boolean removed = false;
    //            while(iterator.hasNext()){
    //                pos = iterator.next();
    //                if(pos.equals(deletePos)){
    //                    iterator.remove();
    //                    removed = true;
    //                    break;
    //                }
    //            }
    //            Assert.assertTrue(removed);
    //            Assert.assertEquals(5 - 1 - j, copyList.size());
    //            while(iterator.hasNext()){
    //                pos = iterator.next();
    //                Assert.assertTrue(! (pos.asLong() == deletePos.asLong() && 
    //                                  pos.getReadId() == deletePos.getReadId() &&
    //                                  pos.getOffset() == deletePos.getOffset()));
    //                i++;
    //            }
    //        }
    //        
    //        //delete all the elements
    //        i = 0;
    //        iterator = plist.iterator();
    //        while(iterator.hasNext()){
    //            pos = iterator.next();
    //            iterator.remove();
    //        }
    //        
    //        Assert.assertEquals(0, plist.size());
    //    }
}
