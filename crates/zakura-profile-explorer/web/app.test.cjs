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

const plain=value=>JSON.parse(JSON.stringify(value));
test('shared verification batches are deduplicated and missing links remain partial',()=>{
  const request=(id)=>({stage:'verification_request',verification:{kind:'request',cache:'miss',primary_batch:id}});
  const batch={stage:'verification_batch',verification:{kind:'batch',id:7}};
  const summary=context.verificationSummary([request(7),request(7),batch,batch],1);
  assert.equal(summary.batches.length,1);
  assert.equal(summary.counts.miss,2);
  assert.equal(summary.partial,false);
  const missing=context.verificationSummary([request(8),batch],1);
  assert.deepEqual(plain(missing.missing),[8]);
  assert.equal(missing.partial,true);
  assert.equal(context.verificationSummary([],undefined).available,false);
});
test('cache-only evidence has no invented execution phases',()=>{
  const summary=context.verificationSummary([{verification:{kind:'request',cache:'hit'}}],1);
  assert.equal(summary.counts.hit,1);
  assert.equal(summary.batches.length,0);
  assert.deepEqual(plain(context.batchPhases({verification:{kind:'batch'}})),[]);
});
test('batch phase intervals preserve separate waiting setup execution and publication',()=>{
  const phases=context.batchPhases({verification:{dispatch_us:10,worker_start_us:20,setup_end_us:25,execution_end_us:50,published_us:55}});
  assert.deepEqual(plain(phases.map(p=>[p.stage,p.start_us,p.end_us])),[
    ['batch_worker_wait',10,20],['batch_setup',20,25],['batch_execution',25,50],['batch_publication',50,55]
  ]);
});
test('shared work before block entry extends the axis without shifting the block',()=>{
  const bounds=context.timelineBounds([{start_us:20,end_us:120}],{start_us:100,end_us:150});
  assert.deepEqual(plain(bounds),{start:20,end:150,duration:130});
  const position=context.intervalPosition({start_us:100,end_us:150},bounds.start,bounds.duration);
  assert.equal(position.left,80/130*100);
  assert.equal(position.width,100-position.left);
  assert.deepEqual(plain(context.intervalPosition({start_us:0,end_us:300},20,130)),{left:0,width:100});
});
test('small durations and pre-block offsets retain microsecond precision',()=>{
  assert.equal(vm.runInContext('ms(6)',context),'6 µs');
  assert.equal(vm.runInContext('ms(-6)',context),'-6 µs');
  assert.equal(vm.runInContext('ms(1200)',context),'1.2 ms');
});

test('rendered transactions default to longest first and can switch to transaction number',()=>{
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
  assert.equal(select.value,'duration');
  assert.deepEqual(labels(),['Transaction 2','Transaction 3','Transaction 1']);
  const expanded=list.children[0];expanded.open=true;
  select.value='number';select.listeners.change();
  assert.deepEqual(labels(),['Transaction 1','Transaction 2','Transaction 3']);
  assert.equal(list.children[1],expanded);
  assert.equal(list.children[1].open,true);
  select.value='duration';select.listeners.change();
  assert.deepEqual(labels(),['Transaction 2','Transaction 3','Transaction 1']);
});
