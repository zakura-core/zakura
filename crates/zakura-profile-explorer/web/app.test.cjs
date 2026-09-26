const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const {test} = require('node:test');

const context = vm.createContext({
  URLSearchParams, location: {search: ''},
  document: {body: {dataset: {page: 'test'}}},
});
// Skip only page startup so the tests exercise the actual UI helpers.
const source = fs.readFileSync(`${__dirname}/app.js`, 'utf8');
vm.runInContext(source.slice(0, source.indexOf("if(document.body.dataset.page==='home')")), context);
const group = spans => JSON.parse(JSON.stringify(context.groupTransactions(spans)));

test('transaction explorer IDs reverse internal bytes without changing the recording', () => {
  const bytes = Array.from({length: 32}, (_, i) => i);
  const original = [...bytes];
  const hash = context.transactionHash(bytes);
  assert.equal(hash, '1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100');
  assert.deepEqual(bytes, original);
  assert.equal(context.explorerUrl('Mainnet', 'tx', hash), `https://cipherscan.app/tx/${hash}`);
  assert.equal(context.explorerUrl('Testnet', 'block', hash.toUpperCase()), `https://testnet.cipherscan.app/block/${hash}`);
});

test('missing or invalid hashes and unsupported networks do not produce links', () => {
  for (const bytes of [undefined, null, [], Array(31).fill(0), Array(32).fill(256), Array(32).fill(-1), Array(32).fill(0.5)]) {
    assert.equal(context.transactionHash(bytes), null);
  }
  for (const hash of [null, undefined, '1'.repeat(63), 'g'.repeat(64), 'https://example.com']) {
    assert.equal(context.explorerUrl('Mainnet', 'tx', hash), null);
  }
  assert.equal(context.explorerUrl('Regtest', 'tx', '1'.repeat(64)), null);
  assert.equal(context.explorerUrl('Mainnet', '../tx', '1'.repeat(64)), null);
});

test('overlapping checks follow the recorded block index, not start order', () => {
  const result = group([
    {span: 3, stage: 'transaction', transaction_index: 1, start_us: 1, end_us: 20},
    {span: 2, stage: 'transaction', transaction_index: 0, start_us: 2, end_us: 10},
    {span: 4, stage: 'worker_execution', transaction_index: 0, start_us: 3, end_us: 30},
    {span: 5, stage: 'transaction_checks', transaction_index: 1, start_us: 4, end_us: 8},
  ]);
  assert.deepEqual(result.groups.map(g => [g.index, g.root.span, g.spans.map(s => s.span)]),
    [[0, 2, [2, 4]], [1, 3, [3, 5]]]);
  assert.deepEqual(result.unassigned, []);
});

test('older recordings retain unassigned checks without guessing ownership', () => {
  const result = group([
    {span: 1, stage: 'transaction', start_us: 1},
    {span: 2, stage: 'transaction', start_us: 2},
    {span: 3, stage: 'transaction_inputs', start_us: 3},
  ]);
  assert.equal(result.groups.length, 0);
  assert.equal(result.legacy.length, 2);
  assert.deepEqual(result.unassigned.map(s => s.span), [3]);
});

test('missing transaction root does not lose or misnumber its checks', () => {
  const result = group([{span: 9, stage: 'transaction_checks', transaction_index: 4}]);
  assert.equal(result.groups[0].index, 4);
  assert.equal(result.groups[0].root, null);
  assert.equal(result.groups[0].spans.length, 1);
});

test('base commit links require a complete Git object ID', () => {
  const sha='a'.repeat(40);
  assert.equal(context.commitUrl(sha.toUpperCase()), `https://github.com/zakura-core/zakura/commit/${sha}`);
  for(const invalid of [null,undefined,'latest main','abc1234','g'.repeat(40),'https://example.com']) {
    assert.equal(context.commitUrl(invalid),null);
  }
});

test('rendered transactions default to transaction number and can switch to longest first',()=>{
  class Element {
    constructor(tag){this.tag=tag;this.children=[];this.listeners={};}
    append(...children){this.children.push(...children);}
    replaceChildren(...children){this.children=children;}
    addEventListener(event,callback){this.listeners[event]=callback;}
    // Like a browser select, use its first option unless explicitly selected.
    get value(){return this.selected ?? (this.tag==='select'?this.children[0]?.value:'');}
    set value(value){this.selected=value;}
  }
  const document={body:{dataset:{page:'test'}},createElement:tag=>new Element(tag)};
  const rendering=vm.createContext({URLSearchParams,location:{search:''},document});
  vm.runInContext(source.slice(0,source.indexOf("if(document.body.dataset.page==='home')")),rendering);
  const host=new Element('div');
  const lane=(_span,tag,label)=>{const node=new Element(tag);node.textContent=label;return node;};
  rendering.renderTransactions(host,[
    {stage:'transaction',transaction_index:0,start_us:0,end_us:5},
    {stage:'transaction',transaction_index:1,start_us:10,end_us:30},
    {stage:'transaction',transaction_index:2,start_us:20,end_us:40},
  ],lane,'Mainnet');
  const select=host.children[0].children[1],list=host.children[1];
  const labels=()=>list.children.map(entry=>entry.children[0].textContent);
  assert.equal(select.value,'number');
  assert.deepEqual(labels(),['Transaction 1','Transaction 2','Transaction 3']);
  const expanded=list.children[1];expanded.open=true;
  select.value='duration';select.listeners.change();
  assert.deepEqual(labels(),['Transaction 2','Transaction 3','Transaction 1']);
  assert.equal(list.children[0],expanded);
  assert.equal(list.children[0].open,true);
  select.value='number';select.listeners.change();
  assert.deepEqual(labels(),['Transaction 1','Transaction 2','Transaction 3']);
});

test('withdrawn verification detail stays out of the restored timeline',()=>{
  const spans=[{stage:'transaction'},{stage:'verification_request'},{stage:'verification_batch'},{stage:'verification_cache'},{stage:'sapling_request'},{stage:'finalization'}];
  assert.deepEqual(JSON.parse(JSON.stringify(context.displayedSpans(spans))).map(s=>s.stage),['transaction','sapling_request','finalization']);
});


test('arrival timeline correlates exact parent and child without fabricating missing milestones',()=>{
  const data={summary:{start_us:1000},dependencies:{events:[{phase:'router_entered',at_us:1000,operation:0}],parent_events:[{phase:'body_received',at_us:400,operation:12},{phase:'router_entered',at_us:2000,operation:0}]}};
  const rows=JSON.parse(JSON.stringify(context.dependencyRows(data)));
  assert.deepEqual(rows.map(r=>[r.block,r.offset]),[['Parent',-600],['This block',0],['Parent',1000]]);
  assert.equal(rows[0].label,'Downloader received body');
  assert.equal(context.dependencyRows({summary:{start_us:0}}).length,0);
});

test('shared proof execution appears once with its actual transaction membership',()=>{
  const spans=[
    {start_us:10,verification:{kind:'batch',id:42,members:4,orchard:4,worker_start_us:20,execution_end_us:4020,status:'success'}},
    ...[2,0,2].map(transaction_index=>({transaction_index,verification:{kind:'request',primary_batch:42,fallback_batch:null}})),
  ];
  const rows=JSON.parse(JSON.stringify(context.sharedProofRows(spans)));
  assert.equal(rows.length,1);
  assert.deepEqual(rows[0].transactions,[0,2]);
  assert.equal(rows[0].elapsed,4000);
  assert.equal(rows[0].other,1);
  assert.equal(rows[0].label,'Orchard proofs + signatures');
  assert.deepEqual(JSON.parse(JSON.stringify(context.sharedProofRows([]))),[]);
});

test('shared proof rows preserve missing timing and fallback evidence',()=>{
  const rows=context.sharedProofRows([
    {start_us:0,verification:{kind:'batch',id:4,members:1,sapling:1,partial:true,status:'abandoned'}},
    {transaction_index:5,verification:{kind:'request',primary_batch:null,fallback_batch:4,fallback:true}},
  ]);
  assert.equal(rows[0].elapsed,null);
  assert.equal(rows[0].partial,true);
  assert.equal(rows[0].fallback,true);
  assert.equal(rows[0].status,'abandoned');
});

test('sync queries retain later outcomes and do not imply unconsumed results are network waits',()=>{
  const rows=JSON.parse(JSON.stringify(context.dependencyRows({summary:{start_us:100},dependencies:{
    events:[{at_us:90,phase:'inventory_sync_response',route:'legacy_peer'}],
    discovery_rounds:[{round:42,events:[{at_us:900,phase:'sync_response_handled',route:'sync',discovery:{round:42,request:1,pending:2,hashes:65}}]}],
  }})));
  assert.equal(rows[0].label,'Inventory used as sync response');
  assert.equal(rows[1].block,'Sync round 42');
  assert.equal(rows[1].offset,800);
  assert.match(rows[1].label,/query 2/);
  assert.match(rows[1].label,/2 query results still to consume/);
  assert.match(rows[1].label,/only first 64 recorded/);
});
