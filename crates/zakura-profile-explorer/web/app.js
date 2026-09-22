/* The UI renders only text nodes from recorded data. No stored data becomes markup. */
const $ = (id) => document.getElementById(id);
const number = (n) => Number(n || 0).toLocaleString();
const ms = (n) => `${(n / 1000).toFixed(1)} ms`;
const el = (tag, text, cls) => { const n = document.createElement(tag); if (text != null) n.textContent = text; if (cls) n.className = cls; return n; };
let selected = '', loading = false;
async function api(path) { const response = await fetch(path); if (!response.ok) throw Error(await response.text()); return response.json(); }
function note(message) { $('notice').textContent = message; $('notice').hidden = !message; }
function quality(row) {
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
  for (const title of ['Block','Hash','Observed','Transactions','Elapsed','Evidence']) head.append(el('th',title));
  const thead = el('thead'); thead.append(head); t.append(thead); const body = el('tbody');
  for (const row of rows) {
    const tr = el('tr'), link = el('a',row.height == null ? 'Unknown height' : number(row.height)); link.href = `#${row.run}/${row.attempt}`;
    link.addEventListener('click', e => { e.preventDefault(); openDetail(row.run,row.attempt).catch(e=>note(e.message)); });
    const first = el('td'); first.append(link); tr.append(first);
    const duration = row.end_us == null ? null : row.end_us-row.start_us;
    tr.append(el('td',row.hash ? `${row.hash.slice(0,12)}…` : '—','hash'),el('td', row.utc_ms ? new Date(row.utc_ms).toLocaleString() : 'Unknown'),el('td',number(row.transactions)),el('td',duration == null ? 'Pending' : ms(duration),duration >= 500000 ? 'slow' : ''),el('td',quality(row)));
    body.append(tr);
  }
  t.append(body);wrap.append(t);host.append(wrap);
}
async function refresh() {
  if (loading) return; loading = true;
  try {
    const query = new URLSearchParams({mode:$('mode').value}); if (selected) query.set('run',selected);
    const data = await api(`/api/home?${query}`); selected = data.run;
    $('run').replaceChildren(...data.runs.map(({metadata:r}) => { const o = el('option',`${r.node || 'Node'} · ${r.session || r.id.slice(0,8)} · ${new Date(r.utc_start_ms).toLocaleString()}`); o.value=r.id; o.selected=r.id===selected; return o; }));
    const run = data.runs.find(r=>r.metadata.id===selected), health = data.health;
    const fresh = health && Date.now()-health.updated_ms<15000, nodeFresh = run && Date.now()-run.seen_ms<10000;
    $('connection').textContent = fresh && nodeFresh ? '● Recording' : '○ Retained evidence';
    const notices = [];
    if (!fresh) notices.push('Collector is offline or its health is stale.');
    if (run && !nodeFresh) notices.push('This node is no longer sending observations.');
    if (run && (run.dropped || run.sequence_gaps || run.transport_dropped)) notices.push(`Collection loss: ${number(run.dropped)} producer drops, ${number(run.sequence_gaps)} missing sequences, ${number(run.transport_dropped)} transport drops. Counts may overlap.`);
    if (health?.errors) notices.push(`${number(health.errors)} collector errors. Detail may be incomplete.`);
    note(notices.join(' '));
    const used = health ? `${(health.used / 1e9).toFixed(2)} / ${(health.budget / 1e9).toFixed(0)} GB` : 'Unavailable';
    const stats = [[number(data.counts.captured),'Requests captured · last 24h'],[number(data.counts.success),'Accepted or checked · last 24h'],[number(data.counts.sealed_detail),'Sealed detail · last 24h'],[used,'Profiler storage · excludes chain state']];
    $('stats').replaceChildren(...stats.map(([value,label])=>{const box=el('div',null,'stat');box.append(el('b',value),el('span',label));return box;}));
    $('cohort').textContent = run ? `${run.metadata.network} · ${run.metadata.storage} · ${run.metadata.build} · ${run.metadata.id}` : 'Waiting for the first node recording.';
    for (const key of ['latest','outliers','failures']) table(key,data[key]);
    $('report').href=`/api/home?${query}`;
  } catch (error) { note(error.message); $('connection').textContent='○ Connection unavailable'; }
  finally { loading=false; }
}
async function openDetail(run,attempt) {
  const data=await api(`/api/attempt/${run}/${attempt}`), row=data.summary;
  $('detail').hidden=false; $('detail-title').textContent=`Block ${number(row.height)} · attempt ${attempt}`;
  $('detail-meta').textContent=`${row.hash} · ${row.outcome || 'unfinished'} · ${quality(row)} · run ${row.run}`;
  history.replaceState(null,'',`#${run}/${attempt}`);
  $('boundary').textContent=data.boundary;
  $('trace').href=`/api/trace/${run}/${attempt}`; $('raw').href=`/api/attempt/${run}/${attempt}`;
  const spans=[...data.spans].sort((a,b)=>a.start_us-b.start_us), start=row.start_us || 0;
  const end=Math.max(row.end_us||start,...spans.map(s=>s.end_us)), duration=Math.max(end-start,1);
  const lanes=[];
  if(row.end_us != null) lanes.push({stage:'Verifier request',start_us:start,end_us:row.end_us,root:true});
  lanes.push(...spans);
  $('timeline').replaceChildren(...lanes.map(s=>{const lane=el('div',null,`lane${s.root?' root':''}`), track=el('div',null,'lane-bar'),bar=el('div',null,'bar');bar.style.left=`${Math.max(0,(s.start_us-start)/duration*100)}%`;bar.style.width=`${Math.max(0,(s.end_us-s.start_us)/duration*100)}%`;bar.title=`${ms(s.start_us-start)} → ${ms(s.end_us-start)}`;track.append(bar);lane.append(el('span',s.stage.replaceAll('_',' '),'lane-name'),track,el('span',ms(s.end_us-s.start_us)));return lane;}));
  renderCpu(data.cpu);
  $('detail').scrollIntoView({behavior:'smooth'});
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
$('run').addEventListener('change',()=>{selected=$('run').value;refresh();});
$('mode').addEventListener('change',refresh);
$('close').addEventListener('click',()=>{$('detail').hidden=true;history.replaceState(null,'',location.pathname);});
$('search').addEventListener('submit',async e=>{e.preventDefault();try{table('results',await api(`/api/search?q=${encodeURIComponent($('query').value.trim())}`));$('search-results').hidden=false;$('search-results').scrollIntoView({behavior:'smooth'});}catch(error){note(error.message);}});
refresh();setInterval(refresh,10000);
const linked=/^#([a-f0-9]{32})\/(\d+)$/.exec(location.hash);
if(linked)openDetail(linked[1],linked[2]).catch(e=>note(e.message));
