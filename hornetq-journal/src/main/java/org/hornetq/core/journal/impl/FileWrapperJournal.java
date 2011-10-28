package org.hornetq.core.journal.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.hornetq.api.core.HornetQException;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.IOCompletion;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.journal.LoaderCallback;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.TransactionFailureCallback;
import org.hornetq.core.journal.impl.dataformat.JournalAddRecord;
import org.hornetq.core.journal.impl.dataformat.JournalAddRecordTX;
import org.hornetq.core.journal.impl.dataformat.JournalCompleteRecordTX;
import org.hornetq.core.journal.impl.dataformat.JournalDeleteRecord;
import org.hornetq.core.journal.impl.dataformat.JournalDeleteRecordTX;
import org.hornetq.core.journal.impl.dataformat.JournalInternalRecord;

/**
 * Journal used at a replicating backup server during the synchronization of data with the 'live'
 * server. It just wraps a single {@link JournalFile}.
 * <p>
 * Its main purpose is to store the data as a Journal would, but without verifying records.
 */
public class FileWrapperJournal extends JournalBase implements Journal
{
   private final ReentrantLock lockAppend = new ReentrantLock();

   private final ConcurrentMap<Long, AtomicInteger> transactions = new ConcurrentHashMap<Long, AtomicInteger>();
   private final JournalImpl journal;
   protected volatile JournalFile currentFile;

   /**
    * @param journal
    * @throws Exception
    */
   public FileWrapperJournal(Journal journal) throws Exception
   {
      super(journal.getFileFactory().isSupportsCallbacks(), journal.getFileSize());
      this.journal = (JournalImpl)journal;
      currentFile = this.journal.setUpCurrentFile(JournalImpl.SIZE_HEADER);
   }

   @Override
   public void start() throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void stop() throws Exception
   {
      if (currentFile.getFile().isOpen())
      currentFile.getFile().close();
   }

   @Override
   public boolean isStarted()
   {
      throw new UnsupportedOperationException();
   }

   // ------------------------

   // ------------------------

   @Override
   public void appendAddRecord(long id, byte recordType, EncodingSupport record, boolean sync, IOCompletion callback)
            throws Exception
   {
      JournalInternalRecord addRecord = new JournalAddRecord(true, id, recordType, record);

      writeRecord(addRecord, sync, callback);
   }

   /**
    * Write the record to the current file.
    */
   private void writeRecord(JournalInternalRecord encoder, final boolean sync, final IOCompletion callback)
            throws Exception
   {

      lockAppend.lock();
      try
      {
         if (callback != null)
         {
            callback.storeLineUp();
         }
         currentFile = journal.switchFileIfNecessary(encoder.getEncodeSize());
         encoder.setFileID(currentFile.getRecordID());

         if (callback != null)
         {
            currentFile.getFile().write(encoder, sync, callback);
         }
         else
         {
            currentFile.getFile().write(encoder, sync);
         }
      }
      finally
      {
         lockAppend.unlock();
      }
   }

   @Override
   public void appendDeleteRecord(long id, boolean sync, IOCompletion callback) throws Exception
   {
      JournalInternalRecord deleteRecord = new JournalDeleteRecord(id);
      writeRecord(deleteRecord, sync, callback);
   }

   @Override
   public void appendDeleteRecordTransactional(long txID, long id, EncodingSupport record) throws Exception
   {
      JournalInternalRecord deleteRecordTX = new JournalDeleteRecordTX(txID, id, record);
      writeRecord(deleteRecordTX, false, null);
   }

   @Override
   public void appendAddRecordTransactional(long txID, long id, byte recordType, EncodingSupport record)
            throws Exception
   {
      count(txID);
      JournalInternalRecord addRecord = new JournalAddRecordTX(true, txID, id, recordType, record);
      writeRecord(addRecord, false, null);
   }

   @Override
   public void
            appendUpdateRecord(long id, byte recordType, EncodingSupport record, boolean sync, IOCompletion callback)
                     throws Exception
   {
      JournalInternalRecord updateRecord = new JournalAddRecord(false, id, recordType, record);
      writeRecord(updateRecord, sync, callback);
   }

   @Override
   public void appendUpdateRecordTransactional(long txID, long id, byte recordType, EncodingSupport record)
            throws Exception
   {
      count(txID);
      JournalInternalRecord updateRecordTX = new JournalAddRecordTX(false, txID, id, recordType, record);
      writeRecord(updateRecordTX, false, null);
   }

   @Override
   public void appendCommitRecord(long txID, boolean sync, IOCompletion callback, boolean lineUpContext)
            throws Exception
   {
      JournalInternalRecord commitRecord = new JournalCompleteRecordTX(true, txID, null);
      AtomicInteger value = transactions.remove(Long.valueOf(txID));
      if (value != null)
      {
         commitRecord.setNumberOfRecords(value.get());
      }

      writeRecord(commitRecord, true, callback);
   }

   @Override
   public void appendPrepareRecord(long txID, EncodingSupport transactionData, boolean sync, IOCompletion callback)
            throws Exception
   {
      JournalInternalRecord prepareRecord = new JournalCompleteRecordTX(false, txID, transactionData);
      AtomicInteger value = transactions.get(Long.valueOf(txID));
      if (value != null)
      {
         prepareRecord.setNumberOfRecords(value.get());
      }
      writeRecord(prepareRecord, sync, callback);
   }

   private int count(long txID) throws HornetQException
   {
      AtomicInteger defaultValue = new AtomicInteger(1);
      AtomicInteger count = transactions.putIfAbsent(Long.valueOf(txID), defaultValue);
      if (count != null)
      {
         return count.incrementAndGet();
      }
      return defaultValue.get();
   }

   @Override
   public String toString()
   {
      return FileWrapperJournal.class.getName() + "(currentFile=[" + currentFile + "], hash=" + super.toString() + ")";
   }

   // UNSUPPORTED STUFF

   @Override
   public void appendRollbackRecord(long txID, boolean sync, IOCompletion callback) throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public JournalLoadInformation load(LoaderCallback reloadManager) throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public JournalLoadInformation loadInternalOnly() throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public void lineUpContex(IOCompletion callback)
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public JournalLoadInformation load(List<RecordInfo> committedRecords,
            List<PreparedTransactionInfo> preparedTransactions, TransactionFailureCallback transactionFailure)
            throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public int getAlignment() throws Exception
   {
      throw new HornetQException(HornetQException.UNSUPPORTED_PACKET);
   }

   @Override
   public int getNumberOfRecords()
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public int getUserVersion()
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void perfBlast(int pages)
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void runDirectJournalBlast() throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public JournalLoadInformation loadSyncOnly() throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public Map<Long, JournalFile> createFilesForBackupSync(long[] fileIds) throws Exception
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void synchronizationLock()
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void synchronizationUnlock()
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void forceMoveNextFile()
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public JournalFile[] getDataFiles()
   {
      throw new UnsupportedOperationException();
   }

   @Override
   void scheduleReclaim()
   {
      // no-op
   }

   @Override
   public SequentialFileFactory getFileFactory()
   {
      throw new UnsupportedOperationException();
   }

}
