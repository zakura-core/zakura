const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const crypto=require('node:crypto');
const {test}=require('node:test');
const context=vm.createContext({document:{body:{dataset:{page:'test'}}}});
vm.runInContext(fs.readFileSync(`${__dirname}/cpu.js`,'utf8'),context);
const plain=value=>JSON.parse(JSON.stringify(value));
test('CPU routes pin an exact recording and never resolve a new height sample',()=>{
  const run='a'.repeat(32);
  const route=context.cpuRoute(`/cpu/${run}/745`);
  assert.deepEqual(plain(route),{run,attempt:'745'});
  assert.equal(context.cpuRoute('/cpu/3493903'),null);
  assert.equal(context.cpuRoute(`/cpu/${run}/745/../../`),null);
  assert.deepEqual(plain(context.cpuEndpoints(route,'verifier')),{samples:`/api/cpu/${run}/745?scope=verifier`,profile:`/api/cpu-profile/${run}/745?scope=verifier`});
  assert.equal(context.cpuScope('https://other.example'),'recorded');
});
test('full-symbol search preserves distinct generic and module identities',()=>{
  const frames=[{name:'verify<Left>',symbol:'raw1',dso:'node'},{name:'verify<Right>',symbol:'raw2',dso:'library'}];
  assert.equal(context.findSymbols(frames,'verify').length,2);
  assert.equal(context.findSymbols(frames,'Right')[0],frames[1]);
  assert.equal(context.findSymbols(frames,'library')[0],frames[1]);
  assert.equal(context.findSymbols(frames,'raw1')[0],frames[0]);
  assert.equal(context.findSymbols(frames,'').length,0);
  assert.equal(context.findSymbols(Array(100).fill(frames[0]),'verify').length,40);
});
test('vendored files match the pinned license and integrity manifest',()=>{
  const directory=`${__dirname}/vendor/speedscope`;
  const manifest=JSON.parse(fs.readFileSync(`${directory}/manifest.json`));
  assert.equal(manifest.commit,'810efdf0a4868bb28f1300f4daacd0db6b8a95bd');
  for(const [file,hash] of Object.entries(manifest.files))assert.equal(crypto.createHash('sha256').update(fs.readFileSync(`${directory}/${file}`)).digest('hex'),hash,file);
  assert.match(fs.readFileSync(`${directory}/LICENSE`,'utf8'),/MIT/);
  const index=fs.readFileSync(`${directory}/index.html`,'utf8');
  assert.doesNotMatch(index,/(?:src|href)=["']https?:/);
});
test('compact labels preserve qualified types and closure context without changing names',()=>{
  const patch=fs.readFileSync(`${__dirname}/vendor/speedscope/compact-labels.patch`,'utf8');
  const added=patch.slice(patch.indexOf('+++ b/src/lib/compact-frame.ts'),patch.indexOf('diff --git',patch.indexOf('+++ b/src/lib/compact-frame.ts')));
  const helper=added.split('\n').filter(line=>line.startsWith('+')&&!line.startsWith('+++')).map(line=>line.slice(1)).join('\n').replace('export function compactFrame(name: string): string','function compactFrame(name)');
  const labels=vm.createContext({});vm.runInContext(helper,labels);
  assert.equal(labels.compactFrame('<orchard::bundle::Bundle<T> as Trait<U>>::verify'),'<orchard::bundle::Bundle<…> as Trait<…>>::verify');
  assert.equal(labels.compactFrame('halo2::plonk::Verifier<Foo<Bar>>::verify'),'halo2::plonk::Verifier<…>::verify');
  assert.equal(labels.compactFrame('tokio::runtime::task::core::Core<T>::poll::{{closure}}'),'tokio::…::Core<…>::poll::{{closure}}');
});

test('CPU time label uses actual period sum, never elapsed time or configured rate',()=>{
  assert.equal(context.cpuWeightLabel({weight:{estimated_cpu_ns:3001001},frequency_hz:99}),'3.00 estimated CPU ms in retained samples');
  assert.equal(context.cpuWeightLabel({weight:{estimated_cpu_ns:null},frequency_hz:999}),'Sample counts only · CPU time weights were not recorded');
  assert.match(context.cpuWeightLabel({}),/Sample counts only/);
  assert.equal(context.cpuWeightLabel({counts:{returned_samples:0}}),'No retained CPU samples in this interval');
});
