/**
 * Backfill CPQ_External_ID__c on custom objects that need it for migration.
 * Uses dynamic SOQL/DML to avoid compile-time schema cache issues on fresh deploys.
 *
 * Pattern: <PREFIX>-<15-char-Id>
 */

// ── Approval_Rule_Product__c ──
try {
    List<SObject> arps = Database.query(
        'SELECT Id, CPQ_External_ID__c FROM Approval_Rule_Product__c WHERE CPQ_External_ID__c = NULL LIMIT 10000'
    );
    if (!arps.isEmpty()) {
        for (SObject arp : arps) {
            arp.put('CPQ_External_ID__c', 'ARP-' + String.valueOf(arp.Id).left(15));
        }
        update arps;
        System.debug('Backfilled CPQ_External_ID__c on ' + arps.size() + ' Approval_Rule_Product__c records');
    } else {
        System.debug('No Approval_Rule_Product__c records need backfill (0 with null CPQ_External_ID__c)');
    }
} catch (Exception e) {
    System.debug('Error on Approval_Rule_Product__c: ' + e.getMessage());
}

// ── Product_Mapping__c ──
try {
    List<SObject> pms = Database.query(
        'SELECT Id, CPQ_External_ID__c FROM Product_Mapping__c WHERE CPQ_External_ID__c = NULL LIMIT 10000'
    );
    if (!pms.isEmpty()) {
        for (SObject pm : pms) {
            pm.put('CPQ_External_ID__c', 'PM-' + String.valueOf(pm.Id).left(15));
        }
        update pms;
        System.debug('Backfilled CPQ_External_ID__c on ' + pms.size() + ' Product_Mapping__c records');
    } else {
        System.debug('No Product_Mapping__c records need backfill (0 with null CPQ_External_ID__c)');
    }
} catch (Exception e) {
    System.debug('Error on Product_Mapping__c: ' + e.getMessage());
}
