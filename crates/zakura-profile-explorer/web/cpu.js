/* CPU weights and elapsed time are separate measurements. */
function cpuRoute(path) {
  const match=/^\/cpu\/([a-f0-9]{32})\/([0-9]{1,20})$/.exec(path);
  return match?{run:match[1],attempt:match[2]}:null;
}
function cpuScope(value) { return value==='verifier'?'verifier':'recorded'; }
function findSymbols(frames,query) {
  const term=query.trim().toLowerCase();
  if(!term)return [];
  return frames.filter(frame=>[frame.name,frame.symbol,frame.dso].some(value=>typeof value==='string'&&value.toLowerCase().includes(term))).slice(0,40);
}
function cpuEndpoints(route,scope,view,span) {
  const selection = view ? `&view=${['context','raw','unassigned'].includes(view)?view:'context'}${/^\d{1,5}$/.test(span || '')?`&span=${span}`:''}` : '';
  const suffix=`${route.run}/${route.attempt}?scope=${cpuScope(scope)}${selection}`;
  return {samples:`/api/cpu/${suffix}`,profile:`/api/cpu-profile/${suffix}`};
}
function cpuWeightLabel(data) {
  if(data.counts?.returned_samples===0)return 'No retained CPU samples in this interval';
  const ns=data.selected_samples!=null ? (data.samples?.every(s=>Number.isFinite(s.cpu_period_ns)) ? data.samples.reduce((sum,s)=>sum+s.cpu_period_ns,0) : null) : data.weight?.estimated_cpu_ns;
  return typeof ns==='number' && Number.isFinite(ns) && ns>=0
    ? `${(ns/1e6).toFixed(2)} estimated CPU ms in retained samples`
    : 'Sample counts only · CPU time weights were not recorded';
}
async function startCpuPage() {
  const $=id=>document.getElementById(id),route=cpuRoute(location.pathname);
  if(!route){$('coverage').textContent='Invalid profile link.';return;}
  $('block-link').href=`/block/${route.run}/${route.attempt}`;
  let frames=[],generation=0,pollTimer,pollDeadline=0;
  function showSymbols(){
    const host=$('symbol-results');host.replaceChildren();
    const matches=findSymbols(frames,$('symbol-query').value);
    for(const frame of matches){
      const row=document.createElement('div');row.className='symbol-result';
      const copy=document.createElement('button');copy.textContent='Copy full symbol';
      copy.addEventListener('click',async()=>{try{await navigator.clipboard.writeText(frame.name || frame.symbol);copy.textContent='Copied';}catch{copy.textContent='Select the symbol to copy';}});
      const name=document.createElement('code');name.textContent=frame.name || frame.symbol;
      const identity=document.createElement('code');identity.textContent=[frame.symbol!==frame.name?frame.symbol:null,frame.dso,frame.ip].filter(Boolean).join(' · ');
      row.append(copy,name,identity);host.append(row);
    }
    if($('symbol-query').value.trim()){const note=document.createElement('p');note.textContent=matches.length===40?'Showing the first 40 matches. Narrow the search for more.':`${matches.length} matching symbols`;host.append(note);}
  }
  $('symbol-query').addEventListener('input',showSymbols);
  function schedulePending(current){
    if(Date.now()>=pollDeadline)return;
    pollTimer=setTimeout(()=>{if(current===generation){if(document.hidden)schedulePending(current);else load(true);}},3000);
  }
  async function load(poll=false){
    clearTimeout(pollTimer);if(!poll)pollDeadline=Date.now()+120000;
    const current=++generation,scope=cpuScope($('scope').value),view=$('view').value,span=$('stage').value,endpoints=cpuEndpoints(route,scope,view,span);
    $('viewer').hidden=true;$('viewer').removeAttribute('src');$('coverage').textContent='Loading retained samples…';
    $('download').href=endpoints.samples;
    history.replaceState(null,'',`${location.pathname}?scope=${scope}&view=${view}${span?`&span=${span}`:''}`);
    try{
      const response=await fetch(endpoints.samples);if(!response.ok)throw Error(await response.text());
      const data=await response.json();if(current!==generation)return;
      frames=data.frames || [];showSymbols();
      const height=data.summary?.height;$('title').textContent=height==null?'CPU profile':`Block ${Number(height).toLocaleString()} · CPU profile`;
      document.title=$('title').textContent+' · Zakura';
      const window=data.window || {},coverage=data.coverage || {},counts=data.counts || {};
      const elapsed=Math.max(0,(window.end_us || 0)-(window.start_us || 0));
      $('interval').textContent=`${scope==='verifier'?'Verifier response':'All recorded work'} · ${(elapsed/1000).toFixed(2)} ms measured elapsed`;
      const count=data.selected_samples ?? counts.returned_samples ?? data.samples?.length ?? 0;
      const problems=[];
      if(counts.omitted_samples)problems.push(`${counts.omitted_samples} omitted samples in capture window`);
      const selectedUnknown=(data.samples || []).filter(sample=>(data.stacks?.[sample.stack] || []).some(id=>data.frames?.[id]?.symbol?.includes('[unknown]'))).length;
      if(selectedUnknown)problems.push(`${selectedUnknown} samples with unknown frames`);
      for(const [key,label] of [['decode_errors','capture decode errors'],['known_lost_samples','known capture losses'],['capture_omitted_samples','capture omissions'],['omitted_frames','capture omitted frames']])if(coverage[key])problems.push(`${coverage[key]} ${label}`);
      if(coverage.query_limited)problems.push('query limit reached');
      const attribution=data.attribution || {};
      $('attribution').textContent=attribution.available ? `${attribution.associated_samples} of ${counts.returned_samples || 0} samples associated with this block · ${attribution.unassigned_samples} unassigned · ${count} shown` : 'No execution associations retained for this recording. Raw process stacks remain available.';
      if(span && !count)$('attribution').textContent+=' No CPU samples were associated with this stage. This does not mean it used no CPU.';
      const selected=$('stage').value;
      $('stage').replaceChildren();
      for(const context of [{span:'',label:'All stages'},...(attribution.contexts || [])]){
        const option=document.createElement('option');option.value=context.span;option.textContent=context.label;$('stage').append(option);
      }
      $('stage').value=selected;
      $('stage').disabled=view==='unassigned';
      $('coverage').dataset.partial=String(problems.length>0);
      $('coverage').textContent=[`${Number(count).toLocaleString()} samples`,data.frequencies_hz?.length?`${data.frequencies_hz.join(' / ')} Hz`:data.frequency_hz?`${data.frequency_hz} Hz`:null,'process-wide',`${coverage.state || 'unknown'} coverage`,...problems].filter(Boolean).join(' · ');
      $('cpu-time').textContent=cpuWeightLabel(data);
      $('capture-reason').textContent=coverage.reason || 'Acquisition coverage is unknown.';
      if(coverage.state==='pending')schedulePending(current);
      if(count>0){
        const profileURL=new URL(endpoints.profile,location.origin).href;
        $('viewer').src=`/cpu-viewer/index.html#profileURL=${encodeURIComponent(profileURL)}&view=left-heavy`;
        $('viewer').hidden=false;
      }
    }catch(error){if(current===generation)$('coverage').textContent=error.message;}
  }
  $('reset-zoom').addEventListener('click',()=>{const viewer=$('viewer');if(viewer.getAttribute('src'))viewer.contentWindow.location.reload();});
  $('scope').value=cpuScope(new URLSearchParams(location.search).get('scope'));
  const params=new URLSearchParams(location.search);
  $('view').value=['context','raw','unassigned'].includes(params.get('view'))?params.get('view'):'context';
  const requested=params.get('span');
  if(/^\d{1,5}$/.test(requested || '')){const option=document.createElement('option');option.value=requested;option.textContent='Selected stage';$('stage').append(option);$('stage').value=requested;}
  $('scope').addEventListener('change',()=>load());
  $('stage').addEventListener('change',()=>load());
  $('view').addEventListener('change',()=>{if($('view').value==='unassigned')$('stage').value='';load();});await load();
}
if(document.body.dataset.page==='cpu')startCpuPage();
