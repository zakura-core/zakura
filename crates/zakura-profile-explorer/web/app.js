/* The UI renders only text nodes from recorded data. No stored data becomes markup. */
const $ = (id) => document.getElementById(id);
const showCpu = new URLSearchParams(location.search).get('cpu') === '1';
const cpuSuffix = showCpu ? '?cpu=1' : '';
const number = (n) => Number(n || 0).toLocaleString();
const ms = (n) => Math.abs(n)<1000 ? `${Math.round(n)} µs` : `${(n / 1000).toFixed(1)} ms`;
const recordingUrl = (run,attempt) => `/block/${encodeURIComponent(run)}/${encodeURIComponent(attempt)}`;
const blockUrl = (row,byHash=false) => row.height!=null && !byHash ? `/block/${row.height}` : row.hash ? `/block/${encodeURIComponent(row.hash)}` : recordingUrl(row.run,row.attempt);
const el = (tag, text, cls) => { const n = document.createElement(tag); if (text != null) n.textContent = text; if (cls) n.className = cls; return n; };
function explorerUrl(network,kind,hash) {
  const host=network==='Mainnet'?'cipherscan.app':network==='Testnet'?'testnet.cipherscan.app':null;
  return host && ['block','tx'].includes(kind) && typeof hash==='string' && /^[a-f0-9]{64}$/i.test(hash)
    ? `https://${host}/${kind}/${hash.toLowerCase()}` : null;
}
function transactionHash(bytes) {
  if(!Array.isArray(bytes) || bytes.length!==32 || !bytes.every(b=>Number.isInteger(b)&&b>=0&&b<=255))return null;
  return [...bytes].reverse().map(b=>b.toString(16).padStart(2,'0')).join('');
}
function commitUrl(sha) {
  return typeof sha==='string' && /^[a-f0-9]{40}$/i.test(sha)
    ? `https://github.com/zakura-core/zakura/commit/${sha.toLowerCase()}` : null;
}
function commitLink(sha,short=false) {
  const url=commitUrl(sha);
  if(!url)return el('span','Not recorded');
  const link=el('a',short?sha.slice(0,9):sha);link.href=url;link.target='_blank';link.rel='noopener noreferrer';link.title=sha;return link;
}
function explorerLink(url,label) {
  const link=el('a',null,'explorer-link');link.href=url;link.target='_blank';link.rel='noopener noreferrer';
  const ns='http://www.w3.org/2000/svg',icon=document.createElementNS(ns,'svg'),circle=document.createElementNS(ns,'circle'),handle=document.createElementNS(ns,'path');
  icon.setAttribute('viewBox','0 0 24 24');icon.setAttribute('aria-hidden','true');icon.setAttribute('focusable','false');
  circle.setAttribute('cx','10.5');circle.setAttribute('cy','10.5');circle.setAttribute('r','6.5');
  handle.setAttribute('d','M16 16l5 5');icon.append(circle,handle);link.append(icon);
  link.title=`View ${label} on CipherScan (opens in a new tab)`;link.setAttribute('aria-label',link.title);
  link.addEventListener('click',event=>event.stopPropagation());return link;
}
let loading = false;
async function api(path) { const response = await fetch(path); if (!response.ok) throw Error(await response.text()); return response.json(); }
function note(message) { $('notice').textContent = message; $('notice').hidden = !message; }
function quality(row) {
  if (row.exclusion_reason) return 'Timing excluded';
  if (row.startup) return 'Startup';
  if (!row.outcome) return 'Unfinished';
  if (row.expired) return 'Detail expired';
  if (row.dropped) return `Truncated · ${number(row.dropped)} omitted`;
  if (row.expected_spans == null) return row.outcome ? 'Detail not sealed' : 'Unfinished';
  if (row.received_spans !== row.expected_spans) return 'Detail missing';
  if (row.expected_spans === 0) return 'Root timing only';
  return 'Recorded spans complete';
}
function table(target, rows) {
  const host = $(target); host.replaceChildren();
  if (!rows.length) { host.append(el('div','No matching recorded requests.', 'empty')); return; }
  const wrap = el('div',null,'table-wrap'), t = el('table'), head = el('tr');
  for (const title of ['Block','Hash','Observed','Transactions','Verifier response','Evidence']) head.append(el('th',title));
  const thead = el('thead'); thead.append(head); t.append(thead); const body = el('tbody');
  for (const row of rows) {
    const tr = el('tr'), link = el('a',row.height == null ? 'Unknown height' : number(row.height)); link.href = blockUrl(row,target==='results')+cpuSuffix;
    const first = el('td'); first.append(link); tr.append(first);
    const duration = row.end_us == null ? null : row.end_us-row.start_us;
    tr.append(el('td',row.hash ? `${row.hash.slice(0,12)}…` : '—','hash'),el('td', row.utc_ms ? new Date(row.utc_ms).toLocaleString() : 'Unknown'),el('td',number(row.transactions)),el('td',row.exclusion_reason ? 'Excluded' : duration == null ? 'Pending' : ms(duration),duration >= 120000 ? 'slow' : ''),el('td',quality(row)));
    body.append(tr);
  }
  t.append(body);wrap.append(t);host.append(wrap);
}
async function refresh() {
  if (loading) return; loading = true;
  try {
    const data = await api('/api/home');
    const run = data.runs.find(r=>r.metadata.id===data.run), health = data.health;
    const fresh = health && Date.now()-health.updated_ms<15000, nodeFresh = run && Date.now()-run.seen_ms<10000;
    const notices = [];
    if (data.startup_pending) notices.push('Node is warming up. Startup blocks are excluded from slow blocks.');
    if (!fresh) notices.push('Collector is offline or its health is stale.');
    if (run && !nodeFresh) notices.push('This node is no longer sending observations.');
    if (health?.errors) notices.push(`${number(health.errors)} collector errors. Detail may be incomplete.`);
    if (data.excluded_timings) notices.push(`${number(data.excluded_timings)} recordings excluded from timing statistics because of known measurement interference.`);
    note(notices.join(' '));
    for (const key of ['latest','outliers','failures']) table(key,data[key]);
  } catch (error) { note(error.message); }
  finally { loading=false; }
}
async function openDetail(run,attempt) {
  const data=await api(`/api/attempt/${run}/${attempt}`), row=data.summary;
  $('lookup').hidden=true;
  $('detail').hidden=false; $('detail-title').textContent=`Block ${number(row.height)}`;
  document.title=`Block ${number(row.height)} · Zakura`;
  renderMetadata(row,data.recording);
  const blockExplorer=explorerUrl(data.recording.network,'block',row.hash);
  $('block-explorer').replaceChildren(...(blockExplorer?[explorerLink(blockExplorer,`block ${number(row.height)}`)]:[]));
  $('detail-warning').hidden=!row.exclusion_reason && !row.startup;
  $('detail-warning').textContent=row.exclusion_reason ? `Timing excluded from rankings and percentiles. ${row.exclusion_reason} Raw intervals are preserved below and include this interference.` : row.startup ? 'Startup profile · excluded from slow blocks and timing statistics.' : '';
  const timing=data.timing, formatTime=value=>value==null?'Pending':ms(value);
  const metrics=row.exclusion_reason
    ? [['Excluded','Processing time'],[formatTime(timing.recorded_elapsed_us),'Raw recorded elapsed · includes interference']]
    : [[formatTime(timing.recorded_elapsed_us),'Total recorded time'],[formatTime(timing.verifier_elapsed_us),'Verifier response'],[formatTime(timing.after_response_us),'Recorded work after response']];
  $('detail-timing').replaceChildren(...metrics.map(([value,label])=>{const box=el('div',null,'stat');box.append(el('b',value),el('span',label));return box;}));
  $('trace').href=`/api/trace/${run}/${attempt}`; $('raw').href=`/api/attempt/${run}/${attempt}`;
  const spans=[...data.spans].sort((a,b)=>a.start_us-b.start_us), start=row.start_us || 0;
  const bounds=timelineBounds(spans,row);
  renderTimeline(spans,row,bounds.start,bounds.duration,data.recording.network,data.recording.verification_detail_version,data.complete);
  if(showCpu){$('cpu').hidden=false;renderCpu(data.cpu);}
}
function renderMetadata(row,recording) {
  $('detail-base').replaceChildren(el('span','Main base · '),commitLink(recording.source?.base_commit,true));
  const retention=/^Pruned\(PruningConfig \{ tx_retention: (\d+) \}\)$/.exec(recording.storage);
  const storage=retention ? `Pruned · transaction data retained for ${number(retention[1])} blocks` : recording.storage;
  const outcome=row.outcome || 'unfinished';
  const fields=[
    ['Block hash',row.hash || 'Unknown',true,true],
    ['Result',outcome[0].toUpperCase()+outcome.slice(1)],
    ['Timing detail',quality(row)],
    ['Network',recording.network],
    ['Storage',storage],
    ['Recorded',new Date(row.utc_ms).toLocaleString(undefined,{dateStyle:'medium',timeStyle:'long'})],
    ['Instrumented build',recording.build],
    ...(recording.source ? [['Instrumented commit',recording.source.commit,true,true]] : []),
    ['Profile ID',row.run,true,true]
  ];
  $('detail-meta').replaceChildren(...fields.map(([label,value,wide,code])=>{
    const item=el('div',null,wide?'metadata-item metadata-wide':'metadata-item'),description=el('dd');
    description.append(el(code?'code':'span',value));item.append(el('dt',label),description);return item;
  }));
}
async function loadBlockPage() {
  const linked=/^\/block\/([a-f0-9]{32})\/(\d{1,20})$/.exec(location.pathname);
  const block=/^\/block\/([0-9]{1,10}|[a-f0-9]{64})$/.exec(location.pathname);
  let query=block?.[1] || (new URLSearchParams(location.search).get('q')||'').trim(), preferredHash;
  if(linked){
    const {summary}=await api(`/api/attempt/${linked[1]}/${linked[2]}`);
    if(summary.height==null && !summary.hash)return openDetail(linked[1],linked[2]);
    query=String(summary.height ?? summary.hash);preferredHash=summary.hash;
  }
  $('query').value=query;
  if(!query){$('lookup-title').textContent='Find a block';$('lookup-copy').textContent='Enter a block height or full hash above to open its latest recorded breakdown.';return;}
  let rows=await api(`/api/search?q=${encodeURIComponent(query)}`);
  // An old link still identifies the same block if multiple forks share its height.
  if(preferredHash && (rows.length!==1 || rows[0].hash!==preferredHash)){query=preferredHash;rows=await api(`/api/search?q=${encodeURIComponent(query)}`);}
  if(rows.length===1){
    history.replaceState(null,'',blockUrl(rows[0],query.length===64)+cpuSuffix);
    return openDetail(rows[0].run,rows[0].attempt);
  }
  $('lookup-title').textContent=rows.length?'Choose a block':'No recorded block found';
  $('lookup-copy').textContent=rows.length?'Different block hashes were recorded at this height. Choose the block to inspect.':'Try another height or full hash. Only retained recordings can be shown.';
  document.title=`${$('lookup-title').textContent} · Zakura`;
  if(rows.length)table('results',rows);
}
const finalizationLabels = {
  finalize_state: 'Move block out of memory', finalize_chain_clone: 'Copy the best chain',
  finalize_root: 'Remove oldest block', finalize_forks: 'Update forks and invalidated blocks',
  finalized_block_prepare: 'Prepare finalized block', finalized_input_prepare: 'Prepare output indexes',
  finalized_parallel_reads: 'Read outputs and serialize transactions', finalized_utxo_read: 'Read spent outputs',
  finalized_serialize: 'Serialize transactions', finalized_address_read: 'Read address balances',
  finalized_batch_prepare: 'Build database batch', finalized_block_batch: 'Encode block and transactions',
  finalized_nullifier_batch: 'Encode nullifiers', finalized_tree_batch: 'Encode trees and anchors',
  finalized_transparent_batch: 'Encode transparent indexes', finalized_value_pool_batch: 'Encode value pools',
  finalized_prune: 'Prepare historical-data pruning', finalized_commit: 'Commit finalized state',
  rocksdb_write: 'Write RocksDB batch', finalization_publication: 'Publish updated state',
  snapshot_clone: 'Copy state snapshot', header_transition_prepare: 'Prepare header transition'
};
const transactionStages = new Set(['transaction','transaction_inputs','transaction_checks','sapling_request','halo2_request','worker_queue','worker_execution']);
function groupTransactions(spans) {
  const indexed=new Map(), legacy=[], unassigned=[];
  for(const span of spans){
    if(Number.isInteger(span.transaction_index) && span.transaction_index>=0){
      if(!indexed.has(span.transaction_index))indexed.set(span.transaction_index,{index:span.transaction_index,spans:[],root:null});
      const group=indexed.get(span.transaction_index);group.spans.push(span);
      if(span.stage==='transaction')group.root=span;
    }else if(span.stage==='transaction')legacy.push(span);
    else unassigned.push(span);
  }
  return {groups:[...indexed.values()].sort((a,b)=>a.index-b.index),legacy:legacy.sort((a,b)=>a.start_us-b.start_us),unassigned};
}
function onFirstExpand(group,render) {
  let rendered=false;
  group.addEventListener('toggle',()=>{if(group.open && !rendered){rendered=true;render();}});
}
function renderTransactions(host,spans,lane,network) {
  const {groups,legacy,unassigned}=groupTransactions(spans);
  const entries=[],list=el('div'),sort=el('select');
  for(const [value,label] of [['number','Transaction number'],['duration','Slowest first']]){
    const option=el('option',label);option.value=value;sort.append(option);
  }
  sort.value='number';
  if(groups.length+legacy.length>1){
    const controls=el('label',null,'transaction-sort');controls.append(el('span','Sort transactions'),sort);host.append(controls);
  }
  if(legacy.length || unassigned.length)host.append(el('p','This older recording did not link checks to transactions. Its transaction numbers reflect verification start order.','muted'));
  host.append(list);
  for(const group of groups){
    const entry=el('details',null,'timeline-group transaction'),body=el('div',null,'timeline-children');
    const root=group.root || group.spans.reduce((bounds,span)=>({start_us:Math.min(bounds.start_us,span.start_us),end_us:Math.max(bounds.end_us,span.end_us)}),{start_us:Infinity,end_us:0});
    const summary=lane(root,'summary',`Transaction ${number(group.index+1)}`);
    const url=explorerUrl(network,'tx',transactionHash(group.root?.transaction_hash));
    if(url)summary.querySelector('.lane-name').append(explorerLink(url,`transaction ${number(group.index+1)}`));
    entry.append(summary,body);
    onFirstExpand(entry,()=>{
      if(!group.root)body.append(el('p','Transaction total was not retained. The bar covers the available checks.','muted'));
      for(const span of group.spans)if(span!==group.root)body.append(lane(span));
      if(group.spans.length===1 && group.root)body.append(el('p','No individual check timings were retained.','muted'));
    });
    entries.push({index:group.index,elapsed:root.end_us-root.start_us,node:entry});
  }
  legacy.forEach((span,index)=>entries.push({index,elapsed:span.end_us-span.start_us,node:lane(span,'div',`Transaction ${number(index+1)}`)}));
  const reorder=()=>list.replaceChildren(...entries.sort((a,b)=>sort.value==='number'?a.index-b.index:b.elapsed-a.elapsed || a.index-b.index).map(entry=>entry.node));
  sort.addEventListener('change',reorder);reorder();
  if(unassigned.length){
    const entry=el('details',null,'timeline-group'),body=el('div',null,'timeline-children');
    entry.append(el('summary','Unassigned transaction checks'),body);
    onFirstExpand(entry,()=>{for(const span of unassigned)body.append(lane(span));});host.append(entry);
  }
  if(!spans.length)host.append(el('p','No individual transaction timings were retained.','muted'));
}
const verificationLabels = {
  verification_request:'Shielded verification request', verification_cache:'Cache lookup',
  verification_ready:'Wait for service readiness', verification_admission:'Wait for admission',
  verification_preparation:'Prepare verification inputs', verification_formation:'Wait for batch flush',
  verification_delivery:'Deliver result', verification_batch:'Shared verification batch'
};
function phaseCategory(stage) {
  if(['verification_ready','verification_admission','verification_formation','batch_flush_wait','batch_worker_wait'].includes(stage))return 'waiting';
  if(['verification_preparation','batch_setup'].includes(stage))return 'preparation';
  if(stage==='batch_execution')return 'execution';
  if(['verification_delivery','batch_publication','verification_cache'].includes(stage))return 'delivery';
  return '';
}
function verificationSummary(spans,version) {
  const requests=spans.filter(s=>s.verification?.kind==='request');
  const batches=new Map();
  for(const span of spans)if(span.verification?.kind==='batch' && !batches.has(span.verification.id))batches.set(span.verification.id,span);
  const counts={hit:0,miss:0,bypass:0,unknown:0};
  for(const request of requests)counts[request.verification.cache in counts?request.verification.cache:'unknown']++;
  const missing=new Set();
  for(const request of requests)for(const id of [request.verification.primary_batch,request.verification.fallback_batch])if(id!=null && !batches.has(id))missing.add(id);
  const partial=missing.size>0 || requests.some(s=>s.verification.partial) || [...batches.values()].some(s=>s.verification.partial || (s.verification.status==='success' && (s.verification.execution_end_us==null || s.verification.published_us==null)));
  return {available:version>=1,requests,batches:[...batches.values()],counts,missing:[...missing],partial};
}
function batchPhases(span) {
  const d=span.verification,phases=[];
  for(const [stage,label,a,b] of [
    ['batch_flush_wait','Wait to dispatch flushed batch',d.flush_us,d.dispatch_us],
    ['batch_worker_wait','Wait for worker',d.dispatch_us,d.worker_start_us],
    ['batch_setup','Worker setup / preparation',d.worker_start_us,d.setup_end_us],
    ['batch_execution','Proofs + signatures',d.setup_end_us ?? d.worker_start_us,d.execution_end_us],
    ['batch_publication','Publish batch result',d.execution_end_us,d.published_us]
  ])if(a!=null && b!=null && b>=a)phases.push({stage,label,start_us:a,end_us:b});
  return phases;
}
function workloadLabel(workload) {
  return [['spends','spends'],['outputs','outputs'],['actions','actions']].filter(([key])=>workload?.[key]>0).map(([key,label])=>`${number(workload[key])} ${label}`).join(' · ') || 'No shielded work';
}
function renderVerification(host,spans,version,lane,complete) {
  const info=verificationSummary(spans,version),group=el('details',null,'timeline-group verification'),body=el('div',null,'timeline-children');
  group.append(el('summary','Shielded verification'),body);host.append(group);
  onFirstExpand(group,()=>{
    if(!info.available){body.append(el('p','This profile predates the detailed verification instrumentation. Existing request timings remain below.','muted'));return;}
    if(!info.requests.length && !info.batches.length){body.append(el('p',complete?'No shielded verification requests were recorded for this block.':'Verification detail is partial. No shielded request detail was retained, so verification cost is unknown.',complete?'muted':'slow'));return;}
    body.append(el('p',`${number(info.counts.hit)} cached · ${number(info.counts.miss)} cache misses · ${number(info.counts.bypass)} cache bypass · ${number(info.counts.unknown)} unknown`,'muted'));
    if(info.partial || !complete)body.append(el('p','Verification detail is partial. Some batch links or phases were not retained.','slow'));
    const legend=el('div',null,'verification-legend');for(const [key,label] of [['preparation','Preparation / setup'],['waiting','Waiting'],['execution','Proofs + signatures'],['delivery','Cache / delivery']])legend.append(el('span',label,`phase-${key}`));body.append(legend);
    if(!info.batches.length)body.append(el('p',info.counts.hit===info.requests.length?'All retained requests used cached results. No fresh execution was recorded.':'No shared execution detail was retained. Fresh verification time is unknown.','muted'));
    const fallback=new Set(info.requests.map(s=>s.verification.fallback_batch).filter(id=>id!=null));
    for(const batch of info.batches.sort((a,b)=>a.start_us-b.start_us)){
      const d=batch.verification,entry=el('details',null,'timeline-group verification-batch'),inside=el('div',null,'timeline-children');
      entry.id=`verification-batch-${d.id}`;
      entry.append(lane(batch,'summary',`${fallback.has(d.id)?'Individual retry':'Shared batch'} ${d.id}`),inside);
      inside.append(el('p',`${d.sapling} Sapling · ${d.orchard} Orchard · ${d.ironwood} Ironwood requests · ${workloadLabel(d.workload)} submitted · ${d.profiled} profiled / ${d.unprofiled} unprofiled · ${d.status}${d.partial?' · Partial detail':''}`,'muted'));
      for(const phase of batchPhases(batch))inside.append(lane(phase,'div',phase.label));
      inside.append(el('p','Elapsed work shared by all members. It is not an exclusive cost of this block or transaction.','muted'));body.append(entry);
    }
    const last=info.requests.filter(s=>s.verification.status!=='abandoned').sort((a,b)=>b.end_us-a.end_us).slice(0,3);
    if(last.length)body.append(el('p',`Last completed shielded requests: ${last.map(s=>`${s.transaction_index==null?'Unassigned':`transaction ${s.transaction_index+1}`} (${s.verification.pool}, ${ms(s.end_us-s.start_us)})`).join('; ')}. This is not a proven critical path.`,'muted'));
  });
}
function requestLinks(span) {
  const d=span.verification,box=el('span',null,'request-links');
  if(d?.kind!=='request')return box;
  box.append(el('span',`${d.pool} · ${d.cache}${d.fallback?' · Individual retry':''} · ${workloadLabel(d.workload)} · ${d.status}${d.partial?' · Partial':''}`));
  for(const [id,label] of [[d.primary_batch,'Batch'],[d.fallback_batch,'Retry']])if(id!=null){
    const link=el('a',`${label} ${id}`);link.href=`#verification-batch-${id}`;
    link.addEventListener('click',()=>{const section=document.querySelector('.verification');if(section)section.open=true;setTimeout(()=>{const target=document.getElementById(`verification-batch-${id}`);if(target){target.open=true;target.scrollIntoView({block:'center'});}},0);});box.append(link);
  }
  return box;
}
function timelineBounds(spans,row) {
  const entry=row.start_us || 0;
  const start=Math.min(entry,...spans.map(s=>s.start_us));
  const end=Math.max(row.end_us || entry,...spans.map(s=>s.end_us));
  return {start,end,duration:Math.max(end-start,1)};
}
function intervalPosition(span,start,duration) {
  const left=Math.max(0,Math.min(100,(span.start_us-start)/duration*100));
  const right=Math.max(left,Math.min(100,(span.end_us-start)/duration*100));
  return {left,width:right-left};
}
function renderTimeline(spans,row,start,duration,network,verificationDetail,complete) {
  const host=$('timeline');host.replaceChildren();
  const finalization=spans.find(s=>s.stage==='finalization'), transactions=spans.find(s=>s.stage==='transactions');
  const children=new Map();
  for(const span of spans){if(!children.has(span.parent))children.set(span.parent,[]);children.get(span.parent).push(span);}
  const finalizationIds=new Set();
  function collect(parent,depth=0){if(depth>8)return;for(const span of children.get(parent)||[]){if(finalizationIds.has(span.span))continue;finalizationIds.add(span.span);collect(span.span,depth+1);}}
  if(finalization)collect(finalization.span);
  const transactionDetail=spans.filter(s=>s.stage!=='verification_batch'&&(transactionStages.has(s.stage)||s.transaction_index!=null)&&!finalizationIds.has(s.span));
  const transactionIds=new Set(transactionDetail.map(s=>s.span));
  const transactionEntry=transactions||transactionDetail[0];
  function lane(span,tag='div',label=verificationLabels[span.stage] || span.stage.replaceAll('_',' ')) {
    const line=el(tag,null,`lane${span.root?' root':''}${phaseCategory(span.stage)?` phase-${phaseCategory(span.stage)}`:''}`),track=el('div',null,'lane-bar'),bar=el('div',null,'bar');
    const position=intervalPosition(span,start,duration);
    bar.style.left=`${position.left}%`;bar.style.width=`${position.width}%`;
    bar.title=`${ms(span.start_us-row.start_us)} → ${ms(span.end_us-row.start_us)} from block entry`;track.append(bar);
    const name=el('span',label,'lane-name');if(span.verification?.kind==='request')name.append(requestLinks(span));
    line.append(name,track,el('span',ms(span.end_us-span.start_us),'lane-time'));return line;
  }
  const ruler=el('div',null,'lane time-ruler'),ticks=el('div',null,'time-ticks');
  for(let i=0;i<=4;i++){const tick=el('span',ms(start+duration*i/4-row.start_us));tick.style.left=`${i*25}%`;ticks.append(tick);}
  ruler.append(el('span','Time from block entry','lane-name'),ticks,el('span','Elapsed','lane-time'));host.append(ruler);
  if(start<row.start_us){const marker=el('div',null,'lane entry-marker'),track=el('div',null,'lane-bar'),line=el('div',null,'entry-line');line.style.left=`${(row.start_us-start)/duration*100}%`;track.append(line);marker.append(el('span','Block entry','lane-name'),track,el('span','0 µs','lane-time'));host.append(marker);}
  if(row.end_us!=null)host.append(lane({stage:'Verifier request',start_us:row.start_us,end_us:row.end_us,root:true}));
  renderVerification(host,spans,verificationDetail,lane,complete);
  for(const span of spans){
    if(span.stage==='verification_batch')continue;
    if(finalizationIds.has(span.span)||(transactionIds.has(span.span)&&span!==transactionEntry))continue;
    if(span===transactionEntry){
      const group=el('details',null,'timeline-group transactions'),body=el('div',null,'timeline-children');
      if(transactions)group.append(lane(span,'summary',`Transactions (${number(row.transactions)})`));
      else{
        const summary=el('summary',null,'lane');
        summary.append(el('span',`Transactions (${number(row.transactions)})`,'lane-name'),el('span','Group timing not recorded','muted'));
        group.append(summary);
      }
      onFirstExpand(group,()=>renderTransactions(body,transactionDetail,lane,network));
      group.append(body);host.append(group);
    }else if(span===finalization){
      const group=el('details',null,'timeline-group finalization'),body=el('div',null,'timeline-children');
      group.append(lane(span,'summary','Finalization'));renderFinalization(body,span,children);group.append(body);host.append(group);
    }else host.append(lane(span));
  }
}
function renderFinalization(host,root,children) {
  host.append(el('p',`${ms(root.end_us-root.start_us)} total. This moves older blocks into finalized storage and can continue after the verifier responds.`,'muted'));
  if(!children.get(root.span)?.length){host.append(el('p','This recording has only the total. A replay with detailed instrumentation is needed to measure its substeps.','muted'));return;}
  const t=el('table'),head=el('tr');for(const title of ['Step','Elapsed','Share of finalization'])head.append(el('th',title));
  const thead=el('thead');thead.append(head);t.append(thead);const body=el('tbody'),seen=new Set([root.span]);
  function visit(parent,depth){if(depth>8)return;for(const span of children.get(parent)||[]){if(seen.has(span.span))continue;seen.add(span.span);const row=el('tr'),name=el('td',`${'↳ '.repeat(depth)}${finalizationLabels[span.stage]||span.stage.replaceAll('_',' ')}`),elapsed=span.end_us-span.start_us;
    row.append(name,el('td',ms(elapsed)),el('td',`${(100*elapsed/Math.max(1,root.end_us-root.start_us)).toFixed(1)}%`));body.append(row);visit(span.span,depth+1);}}
  visit(root.span,0);t.append(body);const wrap=el('div',null,'table-wrap');wrap.append(t);host.append(wrap,el('p','Indented rows are included in their parent. Parallel reads and serialization overlap. Do not add every row together.','muted'));
}
function renderCpu(cpu) {
  const host=$('cpu');host.replaceChildren();
  if(!cpu || cpu.status!=='available'){host.append(el('p',cpu?.reason || 'CPU samples unavailable for this recording.'));return;}
  host.append(el('h2','Sampled CPU stacks'),el('p',`${number(cpu.samples)} samples · ${number(cpu.unknown_samples)} with unknown frames · process scope`),el('p',cpu.scope));
  if(cpu.sparse)host.append(el('p','Sparse sample coverage. This is a hint about activity, not a precise division of this block’s CPU time.','slow'));
  if(cpu.captures.some(c=>c.truncated || c.decode_errors))host.append(el('p','One or more captures were truncated or had decode errors.','slow'));
  const reset=el('button','Reset zoom'), graph=el('div');host.append(reset,graph);
  const tree={name:'Recorded process stacks',count:0,children:new Map()};
  for(const stack of cpu.stacks){let node=tree;node.count+=stack.samples;for(const frame of [...stack.frames].reverse()){if(!node.children.has(frame))node.children.set(frame,{name:frame,count:0,children:new Map()});node=node.children.get(frame);node.count+=stack.samples;}}
  function draw(root){graph.replaceChildren();const ns='http://www.w3.org/2000/svg',svg=document.createElementNS(ns,'svg');svg.setAttribute('viewBox','0 0 1000 420');svg.setAttribute('role','img');svg.setAttribute('aria-label','Process sample flamegraph. Click a frame to zoom.');let drawn=0;
    function branch(node,x,y,width){if(y>390 || width<2 || ++drawn>2000)return;const group=document.createElementNS(ns,'g'),rect=document.createElementNS(ns,'rect'),title=document.createElementNS(ns,'title');rect.setAttribute('x',x);rect.setAttribute('y',y);rect.setAttribute('width',Math.max(0,width-1));rect.setAttribute('height',21);rect.setAttribute('fill',`hsl(${25+(y/23*11)%55} 65% ${52+(y/23)%3*5}%)`);title.textContent=`${node.name} · ${node.count} samples`;group.append(rect,title);if(width>90){const label=document.createElementNS(ns,'text');label.setAttribute('x',x+4);label.setAttribute('y',y+15);label.setAttribute('fill','#151515');label.setAttribute('font-size','11');label.textContent=node.name.slice(0,Math.floor(width/7));group.append(label);}group.addEventListener('click',()=>draw(node));svg.append(group);let offset=x;for(const child of [...node.children.values()].sort((a,b)=>b.count-a.count)){const w=width*child.count/node.count;branch(child,offset,y+23,w);offset+=w;}}
    branch(root,0,0,1000);graph.append(svg,el('p','Widths are sample counts, not milliseconds. Showing the top 200 stacks and up to 18 frames. Click to zoom.'));}
  reset.addEventListener('click',()=>draw(tree));draw(tree);
}
if(document.body.dataset.page==='home'){
  const legacy=/^#([a-f0-9]{32})\/(\d{1,20})$/.exec(location.hash);
  if(legacy)location.replace(recordingUrl(legacy[1],legacy[2])+cpuSuffix);
  else{refresh();setInterval(refresh,10000);}
}else{
  loadBlockPage().catch(error=>{$('lookup-title').textContent='Block unavailable';note(error.message);});
}
