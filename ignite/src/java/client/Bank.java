package client;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.QueryCursor;

import java.util.stream.Collectors;
import javax.cache.Cache;

import java.util.*;

public class Bank extends Client {
    private IgniteCache<Integer, Integer> accountCache;
    private static final String CACHE_NAME = Bank.class.getSimpleName();

    public Bank(String igniteConfig) {
        super(igniteConfig);
    }

    public void setAccountCache(CacheAtomicityMode            atomicityMode,
                                CacheMode                     cacheMode,
                                CacheWriteSynchronizationMode writeSynchronizationMode,
                                Boolean                       readFromBackup,
                                int                           backups) {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>(CACHE_NAME);
        cfg.setAtomicityMode(atomicityMode);
        cfg.setCacheMode(cacheMode);
        cfg.setWriteSynchronizationMode(writeSynchronizationMode);
        cfg.setReadFromBackup(readFromBackup);
        cfg.setBackups(backups);
        accountCache = ignite.getOrCreateCache(cfg);
    }

    public int transferMoney(int                    fromAccountId,
                             int                    toAccountId,
                             TransactionConcurrency transactionConcurrency,
                             TransactionIsolation   transactionIsolation) {

        // Starting the transaction and setting the timeout in order to defreeze the transaction if it gets into
        // the deadlock and to trigger the deadlock detection to troubleshoot the issue.
        // 10 (txSize) is an approximate number of entries participating in transaction.
        Transaction tx = ignite.transactions().txStart(transactionConcurrency, transactionIsolation, TX_TIMEOUT, 10);

        try {
            int fromAccount = accountCache.get(fromAccountId);
            int toAccount = accountCache.get(toAccountId);

            // No money in the account
            if (fromAccount < 1) {
                tx.commit();
                return 0;
            }

            int amount = getRandomNumberInRange(1, fromAccount);

            // Withdraw from account
            fromAccount = fromAccount - amount;

            // Deposit into account.
            toAccount = toAccount + amount;

            // Store updated accounts in cache.
            accountCache.put(fromAccountId, fromAccount);
            accountCache.put(toAccountId, toAccount);

            tx.commit();
            return amount;
        } catch (IgniteException ex){
            tx.rollback();
            throw ex;
        }
    }

    public void updateAccountBalance(int accountId, int balance) {
        accountCache.put(accountId, balance);
    }

    public int getAccountBalance(int accountId) {
        return accountCache.get(accountId);
    }

    private static int getRandomNumberInRange(int min, int max) {
        if (min > max) {
            throw new IllegalArgumentException("max (" + max + ") must be greater than min (" + min + ")");
        }
        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }

   public Map<Integer, Integer> getAllAccounts(int endIndex,
                                                TransactionConcurrency transactionConcurrency,
                                                TransactionIsolation transactionIsolation) {
        Map<Integer, Integer> accounts;
        Transaction tx = ignite.transactions().txStart(transactionConcurrency, transactionIsolation, TX_TIMEOUT, 10);
        try {
            Set<Integer> keys = new HashSet<>();
            for (int i = 0; i < endIndex; i++) {
                keys.add(i);
            }
            accounts = accountCache.getAll(keys);
            tx.commit();
        } catch (IgniteException ex) {
            tx.rollback();
            throw ex;
        } finally {
            tx.close();
        }
        return accounts;
    }

    public Map<Integer, Integer> getAllAccountsScan(int endIndex,
                                                    TransactionConcurrency transactionConcurrency,
                                                    TransactionIsolation transactionIsolation) {
        Map<Integer, Integer> keys;
        Transaction tx = ignite.transactions().txStart(transactionConcurrency, transactionIsolation, TX_TIMEOUT, 10);
        try {
            QueryCursor<Cache.Entry<Integer, Integer>> q = accountCache.query(new ScanQuery(null));
            keys = q.getAll().stream().collect(Collectors.toMap(i -> i.getKey(), i -> i.getValue()));
            tx.commit();
        } catch (IgniteException ex) {
            tx.rollback();
            throw ex;
        } finally {
            tx.close();
        }
        return keys;
    }

    public void destroyCache() {
        ignite.destroyCache(CACHE_NAME);
    }
}
