(self.webpackChunk_N_E=self.webpackChunk_N_E||[]).push([[319],{97434:function(t,n,e){(window.__NEXT_P=window.__NEXT_P||[]).push(["/stations",function(){return e(28340)}])},28340:function(t,n,e){"use strict";e.r(n),e.d(n,{MAPS:function(){return b},__N_SSG:function(){return j},default:function(){return k},ymParam:function(){return w}});var o=e(828),a=e(36305),i=e(85893),r=e(8974),s=e(5152),c=e.n(s)()(function(){return Promise.all([e.e(269),e.e(722)]).then(e.bind(e,14722))},{loadableGenerated:{webpack:function(){return[14722]}},ssr:!1}),u=e(48765),l=e(67294),d=e(33753),m=e.n(d),f=e(83300),p=e.n(f),h=e(96486),g=e.n(h),_={lat:40.758,lng:-73.965},v=Object.entries,y=Object.fromEntries,x=Math.sqrt,j=!0,b={openstreetmap:{url:"https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",attribution:"&copy; <a href=&quot;http://osm.org/copyright&quot;>OpenStreetMap</a> contributors"},alidade_smooth_dark:{url:"https://tiles.stadiamaps.com/tiles/alidade_smooth_dark/{z}/{x}/{y}{r}.png",attribution:'&copy; <a href="https://stadiamaps.com/">Stadia Maps</a>, &copy; <a href="https://openmaptiles.org/">OpenMapTiles</a> &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors'}};function w(t){var n=!(arguments.length>1)||void 0===arguments[1]||arguments[1],e=/^20(\d{4})$/,o=/^\d{4}$/;return{encode:function(n){if(n!=t){var o=n.match(e);return o?o[1]:(console.warn("Invalid ym param: ".concat(n)),void 0)}},decode:function(n){return n?n.match(o)?"20".concat(n):(console.warn("Invalid ym param value: ".concat(n)),t):t},push:n}}function k(t){var n=t.defaults,e=t.stations,s={ll:(0,u.GS)({init:_,places:3}),z:(0,u.yc)(12,!1),ss:(0,u.ON)(),ym:w(n.ym)},d=(0,u.O5)({params:s}),f=(0,o.Z)(d.ll,2),h=f[0],j=h.lat,k=h.lng,M=f[1],C=(0,o.Z)(d.z,2),S=C[0],Z=C[1],N=(0,o.Z)(d.ss,2),P=N[0],L=N[1],O=(0,o.Z)(d.ym,2),T=O[0];O[1];var z=(0,l.useState)(n.stationCounts),E=z[0],q=z[1],A=(0,l.useState)(null),R=A[0],H=A[1],I=(0,l.useMemo)(function(){var t,e=parseInt(T.substring(0,4)),a=parseInt(T.substring(4)),i=new Date(e,a-1).toLocaleDateString("default",{month:"short",year:"numeric"}),r="https://ctbk.s3.amazonaws.com/aggregated/".concat(T),s="".concat(r,"/idx2id.json"),c="".concat(r,"/s_c.json"),u="".concat(r,"/se_c.json");return T==n.ym?(q(n.stationCounts),t=Promise.resolve(n.idx2id),console.log("Setting stationCounts to default")):(console.log("fetching ".concat(c)),t=p()(s).then(function(t){return t.json()}).then(function(t){return console.log("got idx2id"),t}),p()(c).then(function(t){return t.json()}).then(function(n){return t.then(function(t){console.log("got stationCounts"),q(g().mapKeys(n,function(n,e){return t[e]}))})})),console.log("fetching ".concat(u)),p()(u).then(function(t){return t.json()}).then(function(n){return t.then(function(t){console.log("got stationPairCounts"),H(y(v(n).map(function(n){var e=(0,o.Z)(n,2),a=e[0],i=e[1];return[t[a],g().mapKeys(i,function(n,e){return t[e]})]})))})}),{ymString:i}},[T]).ymString,D="Citi Bike rides by station, ".concat(I),F=b.alidade_smooth_dark,G=F.url,B=F.attribution;return(0,i.jsxs)("div",{className:m().container,children:[(0,i.jsx)(r.Z,{title:D,description:"Map of Citi Bike stations, including ridership counts and frequent destinations",path:"stations",thumbnail:"ctbk-stations"}),(0,i.jsxs)("main",{className:m().main,children:[(0,i.jsx)(c,{className:m().homeMap,center:{lat:j,lng:k},zoom:S,zoomControl:!1,zoomSnap:.5,zoomDelta:.5,children:function(t){var n,r,s,c,u,d,f,p,h,g,_,y,j,b,w,k,C,S,N,O,T,z,q,A;return(0,i.jsx)("div",{children:(r={setLL:M,setZoom:Z,stations:e,stationCounts:E,selectedStation:P,setSelectedStation:L,stationPairCounts:R,url:G,attribution:B},s=t.TileLayer,t.Marker,c=t.Circle,t.CircleMarker,u=t.Polyline,d=t.Pane,f=t.Tooltip,p=t.useMapEvents,h=t.useMap,g=r.setLL,_=r.setZoom,y=r.stations,j=r.stationCounts,b=r.selectedStation,w=r.setSelectedStation,k=r.stationPairCounts,C=r.url,S=r.attribution,N=function(t){var n=t.id,e=t.count,o=t.selected,a=y[n],r=a.name,s=a.lat,u=a.lng;return(0,i.jsx)(c,{center:{lat:s,lng:u},color:o?"yellow":"orange",radius:x(e),eventHandlers:{click:function(){n!=b&&(console.log("click:",r),w(n))},mouseover:function(){n!=b&&(console.log("over:",r),w(n))},mousedown:function(){n!=b&&(console.log("down:",r),w(n))}},children:(0,i.jsxs)(f,{sticky:!0,children:[r,": ",e]})},n)},T=(O=h()).getZoom(),z=(0,l.useMemo)(function(){var t,n,e,o,a,i;return n=O.getCenter(),o=[(e=O.latLngToContainerPoint(n)).x+1,e.y],a=O.containerPointToLatLng(e),i=O.containerPointToLatLng(o),a.distanceTo(i)},[O,T,]),p({moveend:function(){return g(O.getCenter())},zoom:function(){return _(O.getZoom())},click:function(){w(void 0)}}),q=(0,l.useMemo)(function(){if(!b||!k)return null;var t,n=k[b],e=Array.from(Object.values(n)),r=(t=Math).max.apply(t,(0,a.Z)(e)),s=j[b],c=y[b];return(0,i.jsx)(d,{name:"lines",className:m().lines,children:v(n).map(function(t){var n=(0,o.Z)(t,2),e=n[0],a=n[1];if(!(e in y)){console.log("id ".concat(e," not in stations:"),y);return}var l=y[e],d=l.name,m=l.lat,p=l.lng;return(0,i.jsx)(u,{color:"red",positions:[[c.lat,c.lng],[m,p],],weight:Math.max(.7,a/r*x(s)/z),opacity:.7,children:(0,i.jsxs)(f,{sticky:!0,children:[c.name," → ",d,": ",a]})},"".concat(b,"-").concat(e,"-").concat(T))})})},[k,b,j,z]),A=b?Array.from(v(j).filter(function(t){var n=(0,o.Z)(t,2),e=n[0];return n[1],e==b}))[0]:void 0,(0,i.jsxs)(i.Fragment,{children:[(0,i.jsx)(d,{name:"selected",className:m().selected,children:A&&[A].map(function(t){var n=(0,o.Z)(t,2),e=n[0],a=n[1];return(0,i.jsx)(N,{id:e,count:a,selected:!0},e)})}),q,(0,i.jsx)(d,{name:"circles",className:m().circles,children:v(j).map(function(t){var n=(0,o.Z)(t,2),e=n[0],a=n[1];return(0,i.jsx)(N,{id:e,count:a},e)})}),(0,i.jsx)(s,{url:C,attribution:S})]}))})}}),D&&(0,i.jsx)("div",{className:m().title,children:D})]})]})}},8974:function(t,n,e){"use strict";e.d(n,{Z:function(){return s}});var o=e(85893);e(67294);var a=e(78855),i="https://ctbk.dev",r="".concat(i,"/screenshots");function s(t){var n=t.title,e=t.description,s=t.path,c=t.thumbnail;return(0,o.jsxs)(a.F,{title:n,description:e,url:"".concat(i,"/").concat(s||""),thumbnail:"".concat(r,"/").concat(c,".png"),children:[(0,o.jsx)("meta",{name:"twitter:card",content:"summary"},"twcard"),(0,o.jsx)("meta",{name:"twitter:creator",content:"RunsAsCoded"},"twhandle"),(0,o.jsx)("meta",{property:"og:site_name",content:"ctbk.dev"},"ogsitename")]})}},33753:function(t){t.exports={container:"stations_container__Z0llF",main:"stations_main__xJuAQ",title:"stations_title__Lq0Hh",homeMap:"stations_homeMap__Ms8dh",selected:"stations_selected__AMT5f",circles:"stations_circles__T0Q2o",lines:"stations_lines__8OJmu"}},83300:function(t,n){"use strict";var e=function(){if("undefined"!=typeof self)return self;if("undefined"!=typeof window)return window;if(void 0!==e)return e;throw Error("unable to locate global object")}();t.exports=n=e.fetch,e.fetch&&(n.default=e.fetch.bind(e)),n.Headers=e.Headers,n.Request=e.Request,n.Response=e.Response}},function(t){t.O(0,[662,171,774,888,179],function(){return t(t.s=97434)}),_N_E=t.O()}]);