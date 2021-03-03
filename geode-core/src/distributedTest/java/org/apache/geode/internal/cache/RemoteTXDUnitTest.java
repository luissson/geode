package org.apache.geode.internal.cache;

import static org.apache.geode.test.dunit.VM.getVM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.Customer;
import org.apache.geode.internal.cache.execute.data.Order;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

public class RemoteTXDUnitTest implements Serializable {

  protected final String CUSTOMER = "custRegion";
  protected final String ORDER = "orderRegion";
  protected final String D_REFERENCE = "distrReference";

  public int initNumElements;
  public OrderId[] initOrderIds;
  public CustId[] initCustomerIds;
  public Customer[] initCustomers;
  public Order[] initOrders;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Before
  public void setUp(){
    getVM(0).invoke(() -> cacheRule.createCache());
    getVM(1).invoke(() -> cacheRule.createCache());
  }

  void initializeVMRegions(boolean isAccessor, int redundantCopies){
    //configure then create partition region for distributed reference
    cacheRule.getCache().createRegionFactory()
        .setScope(Scope.DISTRIBUTED_ACK)
        .setDataPolicy(DataPolicy.REPLICATE)
        .setConcurrencyChecksEnabled(false)
        .create(D_REFERENCE);

    //configure partition region for customers
    PartitionAttributesFactory customerParAttrFac = new PartitionAttributesFactory();
    PartitionAttributes customerParAttr = customerParAttrFac
        .setTotalNumBuckets(4)
        .setLocalMaxMemory(isAccessor ? 0 : 1)
        .setPartitionResolver(new CustomerIDPartitionResolver("resolver1"))
        .setRedundantCopies(redundantCopies).create();

    //configure partition region for orders
    PartitionAttributesFactory orderParAttrFac = new PartitionAttributesFactory();
    PartitionAttributes orderParAttr = orderParAttrFac
        .setTotalNumBuckets(4)
        .setLocalMaxMemory(isAccessor ? 0 : 1)
        .setPartitionResolver(new CustomerIDPartitionResolver("resolver2"))
        .setRedundantCopies(redundantCopies).create();

    //create the customer and order regions regions
    cacheRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(customerParAttr)
        .create(CUSTOMER);

    cacheRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(orderParAttr)
        .create(ORDER);
  }

  void createCustomerAndOrderData(CustId[] customerIds, OrderId[] orderIds,
                                  Customer[] customers, Order[] orders, int numElements) {
    for (int i = 0; i < numElements; i++) {
      CustId customerId = new CustId(i);
      Customer customer = new Customer("customer" + i, "address" + i);
      OrderId orderId = new OrderId(i, customerId);
      Order order = new Order("order" + i);

      orderIds[i] = orderId;
      customerIds[i] = customerId;
      customers[i] = customer;
      orders[i] = order;
    }
  }

  void populateRegions(int initNumElements) {
    Region<CustId, Customer> customerRegion = cacheRule.getCache().getRegion(CUSTOMER);
    Region<OrderId, Order> orderRegion = cacheRule.getCache().getRegion(ORDER);
    Region<CustId, Customer> refRegion = cacheRule.getCache().getRegion(D_REFERENCE);

    OrderId[] orderIds = new OrderId[initNumElements];
    CustId[] customerIds = new CustId[initNumElements];
    Customer[] customers = new Customer[initNumElements];
    Order[] orders = new Order[initNumElements];

    createCustomerAndOrderData(customerIds, orderIds, customers, orders, initNumElements);

    for (int i = 0; i < initNumElements; i++) {
      customerRegion.put(customerIds[i], customers[i]);
      orderRegion.put(orderIds[i], orders[i]);
      refRegion.put(customerIds[i], customers[i]);
    }
  }


  public TXId DoTXPuts() {
    TXManagerImpl txMan = cacheRule.getCache().getTxManager();
    Region<CustId, Customer> customerRegion = cacheRule.getCache().getRegion(CUSTOMER);
    Region<OrderId, Order> orderRegion = cacheRule.getCache().getRegion(ORDER);
    Region<CustId, Customer> refRegion = cacheRule.getCache().getRegion(D_REFERENCE);

    //Begin TX
    txMan.begin();

    //Create data
    CustId customerId = new CustId(1);
    OrderId orderId = new OrderId(1, customerId);
    Customer expectedCustomer = new Customer("Geode", "Beaverton");
    Order expectedOrder = new Order("order");

    //Put data into regions
    customerRegion.put(customerId, expectedCustomer);
    orderRegion.put(orderId, expectedOrder);
    refRegion.put(customerId, expectedCustomer);

    //Region has keys
    assertTrue(customerRegion.containsKey(customerId));
    assertTrue(orderRegion.containsKey(orderId));
    assertTrue(refRegion.containsKey(customerId));

    //Region has values for keys
    assertTrue(customerRegion.containsValueForKey(customerId));
    assertTrue(orderRegion.containsValueForKey(orderId));
    assertTrue(refRegion.containsValueForKey(customerId));

    return (TXId) txMan.suspend();
  }

  public void validateRegionData(CustId[] customerIds, OrderId[] orderIds,
                                 Customer[] customers, Order[] orders) {

    Region<CustId, Customer> customerRegion = cacheRule.getCache().getRegion(CUSTOMER);
    Region<OrderId, Order> orderRegion = cacheRule.getCache().getRegion(ORDER);
    Region<CustId, Customer> refRegion = cacheRule.getCache().getRegion(D_REFERENCE);

    assertEquals(customerIds.length, customerRegion.size());
    assertEquals(orderIds.length, orderRegion.size());
    assertEquals(customerIds.length, refRegion.size());

    //Get and check data from regions
    for(int i = 0; i < customerIds.length; i++){
      assertEquals(customers[i], customerRegion.get(customerIds[i]));
      assertEquals(orders[i], orderRegion.get(orderIds[i]));
      assertEquals(customers[i], refRegion.get(customerIds[i]));

      //Region has values for keys
      assertTrue(customerRegion.containsValueForKey(customerIds[i]));
      assertTrue(orderRegion.containsValueForKey(orderIds[i]));
      assertTrue(refRegion.containsValueForKey(customerIds[i]));
    }
  }

  public TXId DoTXGets(int numElementsPerRegion) {
    TXManagerImpl txMan = cacheRule.getCache().getTxManager();

    txMan.begin();

    OrderId[] orderIds = new OrderId[numElementsPerRegion];
    CustId[] customerIds = new CustId[numElementsPerRegion];
    Customer[] customers = new Customer[numElementsPerRegion];
    Order[] orders = new Order[numElementsPerRegion];

    createCustomerAndOrderData(customerIds, orderIds, customers, orders, numElementsPerRegion);
    validateRegionData(customerIds, orderIds, customers, orders);

    return (TXId) txMan.suspend();
  }

  public void verifyGetsCommit(int numElementsPerRegion) {
    OrderId[] orderIds = new OrderId[numElementsPerRegion];
    CustId[] customerIds = new CustId[numElementsPerRegion];
    Customer[] customers = new Customer[numElementsPerRegion];
    Order[] orders = new Order[numElementsPerRegion];

    createCustomerAndOrderData(customerIds, orderIds, customers, orders, numElementsPerRegion);
    validateRegionData(customerIds, orderIds, customers, orders);
  }

  public boolean isTxInProgress(TXId txId) {
    TXManagerImpl txMan = cacheRule.getCache().getTxManager();
    return txMan.isHostedTxInProgress(txId);
  }

  public void checkTxRegions(TXId txId, int numElementsPerRegion) {
    TXManagerImpl txMan = cacheRule.getCache().getTxManager();
    TXStateProxy tx = txMan.getHostedTXState(txId);
    for(InternalRegion region : tx.getRegions()) {
      assertTrue(region instanceof DistributedRegion);
      TXRegionState regionState = tx.readRegion(region);
      for( Object key : regionState.getEntryKeys()){
        TXEntryState entryState = regionState.readEntry(key);
        assertNotNull(entryState.getValue(key, region, false));
        assertFalse(entryState.isDirty());
      }
    }
  }

  public void completeTx(boolean commit, TXId txId) {
    TXManagerImpl txMan = cacheRule.getCache().getTxManager();
    txMan.resume(txId);
    TXStateProxy tx = txMan.pauseTransaction();
    assertNotNull(tx);
    txMan.unpauseTransaction(tx);
    if(commit) {
      txMan.commit();
    } else {
      txMan.rollback();
    }
  }

  @Test
  public void testBasicTxPutAndCommit() {
    VM accessor = getVM(0);
    VM datastore = getVM(1);

    accessor.invoke(()->initializeVMRegions(true,0));
    datastore.invoke(()->initializeVMRegions(false,0));
    datastore.invoke(()->populateRegions(5));

    TXId txId = accessor.invoke(this::DoTXPuts);

    assertTrue(datastore.invoke(()-> isTxInProgress(txId)));

    accessor.invoke(()->completeTx(true, txId));

    assertFalse(datastore.invoke(()-> isTxInProgress(txId)));
  }

  @Test
  public void testBasicTxPutAndRollback() {
    VM accessor = getVM(0);
    VM datastore = getVM(1);
    accessor.invoke(()->initializeVMRegions(true,0));
    datastore.invoke(()->initializeVMRegions(false,0));
    datastore.invoke(()->populateRegions(5));

    TXId txId = accessor.invoke(this::DoTXPuts);

    assertTrue(datastore.invoke(()-> isTxInProgress(txId)));

    accessor.invoke(()->completeTx(false, txId));

    assertFalse(datastore.invoke(()-> isTxInProgress(txId)));
  }

  @Test
  public void testBasicTxGet() {
    int numElementsPerRegion = 5; // 3 regions
    VM accessor = getVM(0);
    VM datastore = getVM(1);
    accessor.invoke(()->initializeVMRegions(true,0));
    datastore.invoke(()->initializeVMRegions(false,0));
    datastore.invoke(()->populateRegions(numElementsPerRegion));

    final TXId txId = accessor.invoke(()->DoTXGets(numElementsPerRegion));

    assertTrue(datastore.invoke(()-> isTxInProgress(txId)));

    datastore.invoke(()->checkTxRegions(txId, numElementsPerRegion));

    accessor.invoke(()->completeTx(true, txId));

    assertFalse(datastore.invoke(()-> isTxInProgress(txId)));

    accessor.invoke(()->verifyGetsCommit(numElementsPerRegion));
  }


}
