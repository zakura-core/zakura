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
