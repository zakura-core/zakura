/* The UI renders only text nodes from recorded data. No stored data becomes markup. */
const $ = (id) => document.getElementById(id);
const showCpu = new URLSearchParams(location.search).get('cpu') === '1';
const cpuSuffix = showCpu ? '?cpu=1' : '';
const number = (n) => Number(n || 0).toLocaleString();
const ms = (n) => `${(n / 1000).toFixed(1)} ms`;
const recordingUrl = (run,attempt) => `/block/${encodeURIComponent(run)}/${encodeURIComponent(attempt)}`;
const blockUrl = (row,byHash=false) => row.height!=null && !byHash ? `/block/${row.height}` : row.hash ? `/block/${encodeURIComponent(row.hash)}` : recordingUrl(row.run,row.attempt);
const el = (tag, text, cls) => { const n = document.createElement(tag); if (text != null) n.textContent = text; if (cls) n.className = cls; return n; };
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
  $('detail-warning').hidden=!row.exclusion_reason && !row.startup;
  $('detail-warning').textContent=row.exclusion_reason ? `Timing excluded from rankings and percentiles. ${row.exclusion_reason} Raw intervals are preserved below and include this interference.` : row.startup ? 'Startup profile · excluded from slow blocks and timing statistics.' : '';
  const timing=data.timing, formatTime=value=>value==null?'Pending':ms(value);
  const metrics=row.exclusion_reason
    ? [['Excluded','Processing time'],[formatTime(timing.recorded_elapsed_us),'Raw recorded elapsed · includes interference']]
    : [[formatTime(timing.recorded_elapsed_us),'Total recorded time'],[formatTime(timing.verifier_elapsed_us),'Verifier response'],[formatTime(timing.after_response_us),'Recorded work after response']];
  $('detail-timing').replaceChildren(...metrics.map(([value,label])=>{const box=el('div',null,'stat');box.append(el('b',value),el('span',label));return box;}));
  $('trace').href=`/api/trace/${run}/${attempt}`; $('raw').href=`/api/attempt/${run}/${attempt}`;
  const spans=[...data.spans].sort((a,b)=>a.start_us-b.start_us), start=row.start_us || 0;
  const end=Math.max(row.end_us||start,...spans.map(s=>s.end_us)), duration=Math.max(end-start,1);
  renderTimeline(spans,row,start,duration);
  if(showCpu){$('cpu').hidden=false;renderCpu(data.cpu);}
}
function renderMetadata(row,recording) {
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
    ['Node build',recording.build],
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
function renderTimeline(spans,row,start,duration) {
  const host=$('timeline');host.replaceChildren();
  const finalization=spans.find(s=>s.stage==='finalization'), transactions=spans.find(s=>s.stage==='transactions');
  const children=new Map();
  for(const span of spans){if(!children.has(span.parent))children.set(span.parent,[]);children.get(span.parent).push(span);}
  const finalizationIds=new Set();
  function collect(parent,depth=0){if(depth>8)return;for(const span of children.get(parent)||[]){if(finalizationIds.has(span.span))continue;finalizationIds.add(span.span);collect(span.span,depth+1);}}
  if(finalization)collect(finalization.span);
  const transactionDetail=spans.filter(s=>transactionStages.has(s.stage)&&!finalizationIds.has(s.span));
  const transactionIds=new Set(transactionDetail.map(s=>s.span));
  const transactionEntry=transactions||transactionDetail[0];
  function lane(span,tag='div',label=span.stage.replaceAll('_',' ')) {
    const line=el(tag,null,`lane${span.root?' root':''}`),track=el('div',null,'lane-bar'),bar=el('div',null,'bar');
    bar.style.left=`${Math.max(0,(span.start_us-start)/duration*100)}%`;bar.style.width=`${Math.max(0,(span.end_us-span.start_us)/duration*100)}%`;
    bar.title=`${ms(span.start_us-start)} → ${ms(span.end_us-start)}`;track.append(bar);
    line.append(el('span',label,'lane-name'),track,el('span',ms(span.end_us-span.start_us),'lane-time'));return line;
  }
  if(row.end_us!=null)host.append(lane({stage:'Verifier request',start_us:start,end_us:row.end_us,root:true}));
  for(const span of spans){
    if(finalizationIds.has(span.span)||(transactionIds.has(span.span)&&span!==transactionEntry))continue;
    if(span===transactionEntry){
      const group=el('details',null,'timeline-group transactions'),body=el('div',null,'timeline-children');
      if(transactions)group.append(lane(span,'summary',`Transactions (${number(row.transactions)})`));
      else{
        const summary=el('summary',null,'lane');
        summary.append(el('span',`Transactions (${number(row.transactions)})`,'lane-name'),el('span','Group timing not recorded','muted'));
        group.append(summary);
      }
      if(transactionDetail.length)body.append(...transactionDetail.map(s=>lane(s)));else body.append(el('p','No individual transaction timings were retained.','muted'));
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
