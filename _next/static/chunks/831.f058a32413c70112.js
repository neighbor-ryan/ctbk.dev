"use strict";(self.webpackChunk_N_E=self.webpackChunk_N_E||[]).push([[831],{25831:function(e,t,r){r.r(t),r.d(t,{Tooltip:function(){return ea},TooltipProvider:function(){return d},TooltipWrapper:function(){return h}});var n=r(67294),o={exports:{}},l={};!function(){var e=Symbol.for("react.element"),t=Symbol.for("react.portal"),r=Symbol.for("react.fragment"),o=Symbol.for("react.strict_mode"),i=Symbol.for("react.profiler"),a=Symbol.for("react.provider"),s=Symbol.for("react.context"),c=Symbol.for("react.forward_ref"),u=Symbol.for("react.suspense"),f=Symbol.for("react.suspense_list"),p=Symbol.for("react.memo"),d=Symbol.for("react.lazy"),y=Symbol.for("react.offscreen"),h=Symbol.iterator,m=n.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED;function g(e){for(var t,r,n,o,l,i=arguments.length,a=Array(i>1?i-1:0),s=1;s<i;s++)a[s-1]=arguments[s];t="error",r=e,n=a,o=m.ReactDebugCurrentFrame.getStackAddendum(),""!==o&&(r+="%s",n=n.concat([o])),l=n.map(function(e){return String(e)}),l.unshift("Warning: "+r),Function.prototype.apply.call(console[t],console,l)}function v(e){return e.displayName||"Context"}function w(e){if(null==e)return null;if("number"==typeof e.tag&&g("Received an unexpected object in getComponentNameFromType(). This is likely a bug in React. Please file an issue."),"function"==typeof e)return e.displayName||e.name||null;if("string"==typeof e)return e;switch(e){case r:return"Fragment";case t:return"Portal";case i:return"Profiler";case o:return"StrictMode";case u:return"Suspense";case f:return"SuspenseList"}if("object"==typeof e)switch(e.$$typeof){case s:return v(e)+".Consumer";case a:return v(e._context)+".Provider";case c:return function(e,t,r){var n=e.displayName;if(n)return n;var o=t.displayName||t.name||"";return""!==o?r+"("+o+")":r}(e,e.render,"ForwardRef");case p:var n=e.displayName||null;return null!==n?n:w(e.type)||"Memo";case d:var l=e._payload,y=e._init;try{return w(y(l))}catch(h){}}return null}b=Symbol.for("react.module.reference");var b,x,S,_,R,k,T,O,E=Object.assign,j=0;function A(){}A.__reactDisabledLog=!0;var P,N=m.ReactCurrentDispatcher;function C(e,t,r){if(void 0===P)try{throw Error()}catch(o){var n=o.stack.trim().match(/\n( *(at )?)/);P=n&&n[1]||""}return"\n"+P+e}var L,$=!1,D="function"==typeof WeakMap?WeakMap:Map;function W(e,t){if(!e||$)return"";var r,n=L.get(e);if(void 0!==n)return n;$=!0;var o,l=Error.prepareStackTrace;Error.prepareStackTrace=void 0,o=N.current,N.current=null,function(){if(0===j){x=console.log,S=console.info,_=console.warn,R=console.error,k=console.group,T=console.groupCollapsed,O=console.groupEnd;var e={configurable:!0,enumerable:!0,value:A,writable:!0};Object.defineProperties(console,{info:e,log:e,warn:e,error:e,group:e,groupCollapsed:e,groupEnd:e})}j++}();try{if(t){var i=function(){throw Error()};if(Object.defineProperty(i.prototype,"props",{set:function(){throw Error()}}),"object"==typeof Reflect&&Reflect.construct){try{Reflect.construct(i,[])}catch(a){r=a}Reflect.construct(e,[],i)}else{try{i.call()}catch(s){r=s}e.call(i.prototype)}}else{try{throw Error()}catch(c){r=c}e()}}catch(h){if(h&&r&&"string"==typeof h.stack){for(var u=h.stack.split("\n"),f=r.stack.split("\n"),p=u.length-1,d=f.length-1;p>=1&&d>=0&&u[p]!==f[d];)d--;for(;p>=1&&d>=0;p--,d--)if(u[p]!==f[d]){if(1!==p||1!==d)do if(p--,--d<0||u[p]!==f[d]){var y="\n"+u[p].replace(" at new "," at ");return e.displayName&&y.includes("<anonymous>")&&(y=y.replace("<anonymous>",e.displayName)),"function"==typeof e&&L.set(e,y),y}while(p>=1&&d>=0);break}}}finally{$=!1,N.current=o,function(){if(0==--j){var e={configurable:!0,enumerable:!0,writable:!0};Object.defineProperties(console,{log:E({},e,{value:x}),info:E({},e,{value:S}),warn:E({},e,{value:_}),error:E({},e,{value:R}),group:E({},e,{value:k}),groupCollapsed:E({},e,{value:T}),groupEnd:E({},e,{value:O})})}j<0&&g("disabledDepth fell below zero. This is a bug in React. Please file an issue.")}(),Error.prepareStackTrace=l}var m=e?e.displayName||e.name:"",v=m?C(m):"";return"function"==typeof e&&L.set(e,v),v}function F(e,t,r){if(null==e)return"";if("function"==typeof e)return W(e,!(!(n=e.prototype)||!n.isReactComponent));if("string"==typeof e)return C(e);switch(e){case u:return C("Suspense");case f:return C("SuspenseList")}if("object"==typeof e)switch(e.$$typeof){case c:return W(e.render,!1);case p:return F(e.type,t,r);case d:var n,o=e._payload,l=e._init;try{return F(l(o),t,r)}catch(i){}}return""}L=new D;var I=Object.prototype.hasOwnProperty,H={},B=m.ReactDebugCurrentFrame;function M(e){if(e){var t=e._owner,r=F(e.type,e._source,t?t.type:null);B.setExtraStackFrame(r)}else B.setExtraStackFrame(null)}var U=Array.isArray;function V(e){if(function(e){try{return!1}catch(t){return!0}}(e)){var t;return g("The provided key is an unsupported type %s. This value must be coerced to a string before before using it here.","function"==typeof Symbol&&Symbol.toStringTag&&e[Symbol.toStringTag]||e.constructor.name||"Object"),""+e}}var z,Y,q,X=m.ReactCurrentOwner,K={key:!0,ref:!0,__self:!0,__source:!0};q={};var J,Z=m.ReactCurrentOwner,G=m.ReactDebugCurrentFrame;function Q(e){if(e){var t=e._owner,r=F(e.type,e._source,t?t.type:null);G.setExtraStackFrame(r)}else G.setExtraStackFrame(null)}function ee(t){return"object"==typeof t&&null!==t&&t.$$typeof===e}function et(){if(Z.current){var e=w(Z.current.type);if(e)return"\n\nCheck the render method of `"+e+"`."}return""}J=!1;var er={};function en(e,t){if(e._store&&!e._store.validated&&null==e.key){e._store.validated=!0;var r=function(e){var t=et();if(!t){var r="string"==typeof e?e:e.displayName||e.name;r&&(t="\n\nCheck the top-level render call using <"+r+">.")}return t}(t);if(!er[r]){er[r]=!0;var n="";e&&e._owner&&e._owner!==Z.current&&(n=" It was passed a child from "+w(e._owner.type)+"."),Q(e),g('Each child in a list should have a unique "key" prop.%s%s See https://reactjs.org/link/warning-keys for more information.',r,n),Q(null)}}}function eo(e,t){if("object"==typeof e){var r;if(U(e))for(var n=0;n<e.length;n++){var o=e[n];ee(o)&&en(o,t)}else if(ee(e))e._store&&(e._store.validated=!0);else if(e){var l=function(e){if(null===e||"object"!=typeof e)return null;var t=h&&e[h]||e["@@iterator"];return"function"==typeof t?t:null}(e);if("function"==typeof l&&l!==e.entries)for(var i,a=l.call(e);!(i=a.next()).done;)ee(i.value)&&en(i.value,t)}}}function el(t,n,l,h,m,v){var x,S="string"==typeof t||"function"==typeof t||t===r||t===i||t===o||t===u||t===f||t===y||"object"==typeof t&&null!==t&&(t.$$typeof===d||t.$$typeof===p||t.$$typeof===a||t.$$typeof===s||t.$$typeof===c||t.$$typeof===b||void 0!==t.getModuleId);if(!S){var _,R,k,T="";(void 0===t||"object"==typeof t&&null!==t&&0===Object.keys(t).length)&&(T+=" You likely forgot to export your component from the file it's defined in, or you might have mixed up default and named imports."),T+=(void 0!==m?"\n\nCheck your code at "+m.fileName.replace(/^.*[\\\/]/,"")+":"+m.lineNumber+".":"")||et(),null===t?R="null":U(t)?R="array":void 0!==t&&t.$$typeof===e?(R="<"+(w(t.type)||"Unknown")+" />",T=" Did you accidentally export a JSX literal instead of a component?"):R=typeof t,g("React.jsx: type is invalid -- expected a string (for built-in components) or a class/function (for composite components) but got: %s.%s",R,T)}var O=function(t,r,n,o,l){var i,a,s,c,u,f,p,d,y,h={},m=null,v=null;for(y in void 0!==n&&(V(n),m=""+n),function(e){if(I.call(e,"key")){var t=Object.getOwnPropertyDescriptor(e,"key").get;if(t&&t.isReactWarning)return!1}return void 0!==e.key}(r)&&(V(r.key),m=""+r.key),function(e){if(I.call(e,"ref")){var t=Object.getOwnPropertyDescriptor(e,"ref").get;if(t&&t.isReactWarning)return!1}return void 0!==e.ref}(r)&&(v=r.ref,function(e,t){if("string"==typeof e.ref&&X.current&&t&&X.current.stateNode!==t){var r=w(X.current.type);q[r]||(g('Component "%s" contains the string ref "%s". Support for string refs will be removed in a future major release. This case cannot be automatically converted to an arrow function. We ask you to manually fix this case by using useRef() or createRef() instead. Learn more about using refs safely here: https://reactjs.org/link/strict-mode-string-ref',w(X.current.type),e.ref),q[r]=!0)}}(r,l)),r)I.call(r,y)&&!K.hasOwnProperty(y)&&(h[y]=r[y]);if(t&&t.defaultProps){var b=t.defaultProps;for(y in b)void 0===h[y]&&(h[y]=b[y])}if(m||v){var x,S,_,R,k,T,O="function"==typeof t?t.displayName||t.name||"Unknown":t;m&&((_=function(){z||(z=!0,g("%s: `key` is not a prop. Trying to access it will result in `undefined` being returned. If you need to access the same value within the child component, you should pass it as a different prop. (https://reactjs.org/link/special-props)",O))}).isReactWarning=!0,Object.defineProperty(h,"key",{get:_,configurable:!0})),v&&((T=function(){Y||(Y=!0,g("%s: `ref` is not a prop. Trying to access it will result in `undefined` being returned. If you need to access the same value within the child component, you should pass it as a different prop. (https://reactjs.org/link/special-props)",O))}).isReactWarning=!0,Object.defineProperty(h,"ref",{get:T,configurable:!0}))}return a=m,s=v,Object.defineProperty((d={$$typeof:e,type:t,key:a,ref:s,props:h,_owner:f=X.current,_store:{}})._store,"validated",{configurable:!1,enumerable:!1,writable:!0,value:!1}),Object.defineProperty(d,"_self",{configurable:!1,enumerable:!1,writable:!1,value:l}),Object.defineProperty(d,"_source",{configurable:!1,enumerable:!1,writable:!1,value:o}),Object.freeze&&(Object.freeze(d.props),Object.freeze(d)),d}(t,n,l,m,v);if(null==O)return O;if(S){var E,j=n.children;if(void 0!==j){if(h){if(U(E=j)){for(var A=0;A<j.length;A++)eo(j[A],t);Object.freeze&&Object.freeze(j)}else g("React.jsx: Static children should always be an array. You are likely explicitly calling React.jsxs or React.jsxDEV. Use the Babel transform instead.")}else eo(j,t)}}return t===r?function(e){for(var t=Object.keys(e.props),r=0;r<t.length;r++){var n=t[r];if("children"!==n&&"key"!==n){Q(e),g("Invalid prop `%s` supplied to `React.Fragment`. React.Fragment can only have `key` and `children` props.",n),Q(null);break}}null!==e.ref&&(Q(e),g("Invalid attribute `ref` supplied to `React.Fragment`."),Q(null))}(O):function(e){var t,r=e.type;if(null!=r&&"string"!=typeof r){if("function"==typeof r)t=r.propTypes;else{if("object"!=typeof r||r.$$typeof!==c&&r.$$typeof!==p)return;t=r.propTypes}if(t){var n=w(r);!function(e,t,r,n,o){var l=Function.call.bind(I);for(var i in e)if(l(e,i)){var a=void 0;try{if("function"!=typeof e[i]){var s=Error((n||"React class")+": "+r+" type `"+i+"` is invalid; it must be a function, usually from the `prop-types` package, but received `"+typeof e[i]+"`.This often happens because of typos such as `PropTypes.function` instead of `PropTypes.func`.");throw s.name="Invariant Violation",s}a=e[i](t,i,n,r,null,"SECRET_DO_NOT_PASS_THIS_OR_YOU_WILL_BE_FIRED")}catch(c){a=c}!a||a instanceof Error||(M(o),g("%s: type specification of %s `%s` is invalid; the type checker function must return `null` or an `Error` but returned a %s. You may have forgotten to pass an argument to the type checker creator (arrayOf, instanceOf, objectOf, oneOf, oneOfType, and shape all require an argument).",n||"React class",r,i,typeof a),M(null)),a instanceof Error&&!(a.message in H)&&(H[a.message]=!0,M(o),g("Failed %s type: %s",r,a.message),M(null))}}(t,e.props,"prop",n,e)}else void 0===r.PropTypes||J||(J=!0,g("Component %s declared `PropTypes` instead of `propTypes`. Did you misspell the property assignment?",w(r)||"Unknown"));"function"!=typeof r.getDefaultProps||r.getDefaultProps.isReactClassApproved||g("getDefaultProps is only used on classic React.createClass definitions. Use a static property named `defaultProps` instead.")}}(O),O}var ei=function(e,t,r){return el(e,t,r,!1)},ea=function(e,t,r){return el(e,t,r,!0)};l.Fragment=r,l.jsx=ei,l.jsxs=ea}(),o.exports=l;var i,a={exports:{}};/*!
	Copyright (c) 2018 Jed Watson.
	Licensed under the MIT License (MIT), see
	http://jedwatson.github.io/classnames
*/ i=a,function(){var e={}.hasOwnProperty;function t(){for(var r=[],n=0;n<arguments.length;n++){var o=arguments[n];if(o){var l=typeof o;if("string"===l||"number"===l)r.push(o);else if(Array.isArray(o)){if(o.length){var i=t.apply(null,o);i&&r.push(i)}}else if("object"===l){if(o.toString!==Object.prototype.toString&&!o.toString.toString().includes("[native code]")){r.push(o.toString());continue}for(var a in o)e.call(o,a)&&o[a]&&r.push(a)}}}return r.join(" ")}i.exports?(t.default=t,i.exports=t):window.classNames=t}();var s=a.exports;let c=(e,t,r)=>{let n=null;return function(...o){n&&clearTimeout(n),n=setTimeout(()=>{n=null,r||e.apply(this,o)},t)}},u=({content:e})=>o.exports.jsx("span",{dangerouslySetInnerHTML:{__html:e}}),f={anchorRefs:new Set,activeAnchor:{current:null},attach(){},detach(){},setActiveAnchor(){}},p=(0,n.createContext)(Object.assign(()=>f,f)),d=({children:e})=>{let t=(0,n.useId)(),[r,l]=(0,n.useState)({[t]:new Set}),[i,a]=(0,n.useState)({[t]:{current:null}}),s=(e,...t)=>{l(r=>{var n;let o=null!==(n=r[e])&&void 0!==n?n:new Set;return t.forEach(e=>o.add(e)),{...r,[e]:new Set(o)}})},c=(e,...t)=>{l(r=>{let n=r[e];return n?(t.forEach(e=>n.delete(e)),{...r}):r})},u=(0,n.useCallback)(e=>{var n,o;return{anchorRefs:null!==(n=r[null!=e?e:t])&&void 0!==n?n:new Set,activeAnchor:null!==(o=i[null!=e?e:t])&&void 0!==o?o:{current:null},attach:(...r)=>s(null!=e?e:t,...r),detach:(...r)=>c(null!=e?e:t,...r),setActiveAnchor(r){var n,o;return n=null!=e?e:t,void a(e=>{var t;return(null===(t=e[n])||void 0===t?void 0:t.current)===r.current?e:{...e,[n]:r}})}}},[t,r,i,s,c]),f=(0,n.useMemo)(()=>{let e=u(t);return Object.assign(e=>u(e),e)},[u]);return o.exports.jsx(p.Provider,{value:f,children:e})};function y(){return(0,n.useContext)(p)}let h=({tooltipId:e,children:t,place:r,content:l,html:i,variant:a,offset:s,wrapper:c,events:u,positionStrategy:f,delayShow:p,delayHide:d})=>{let{attach:h,detach:m}=y()(e),g=(0,n.useRef)(null);return(0,n.useEffect)(()=>(h(g),()=>{m(g)}),[]),o.exports.jsx("span",{ref:g,"data-tooltip-place":r,"data-tooltip-content":l,"data-tooltip-html":i,"data-tooltip-variant":a,"data-tooltip-offset":s,"data-tooltip-wrapper":c,"data-tooltip-events":u,"data-tooltip-position-strategy":f,"data-tooltip-delay-show":p,"data-tooltip-delay-hide":d,children:t})};function m(e){return e.split("-")[0]}function g(e){return e.split("-")[1]}function v(e){return["top","bottom"].includes(m(e))?"x":"y"}function w(e){return"y"===e?"height":"width"}function b(e,t,r){let{reference:n,floating:o}=e,l=n.x+n.width/2-o.width/2,i=n.y+n.height/2-o.height/2,a=v(t),s=w(a),c=n[s]/2-o[s]/2,u="x"===a,f;switch(m(t)){case"top":f={x:l,y:n.y-o.height};break;case"bottom":f={x:l,y:n.y+n.height};break;case"right":f={x:n.x+n.width,y:i};break;case"left":f={x:n.x-o.width,y:i};break;default:f={x:n.x,y:n.y}}switch(g(t)){case"start":f[a]-=c*(r&&u?-1:1);break;case"end":f[a]+=c*(r&&u?-1:1)}return f}function x(e){var t;return"number"!=typeof e?{top:0,right:0,bottom:0,left:0,...e}:{top:e,right:e,bottom:e,left:e}}function S(e){return{...e,top:e.y,left:e.x,right:e.x+e.width,bottom:e.y+e.height}}async function _(e,t){var r;void 0===t&&(t={});let{x:n,y:o,platform:l,rects:i,elements:a,strategy:s}=e,{boundary:c="clippingAncestors",rootBoundary:u="viewport",elementContext:f="floating",altBoundary:p=!1,padding:d=0}=t,y=x(d),h=a[p?"floating"===f?"reference":"floating":f],m=S(await l.getClippingRect({element:null==(r=await (null==l.isElement?void 0:l.isElement(h)))||r?h:h.contextElement||await (null==l.getDocumentElement?void 0:l.getDocumentElement(a.floating)),boundary:c,rootBoundary:u,strategy:s})),g=S(l.convertOffsetParentRelativeRectToViewportRelativeRect?await l.convertOffsetParentRelativeRectToViewportRelativeRect({rect:"floating"===f?{...i.floating,x:n,y:o}:i.reference,offsetParent:await (null==l.getOffsetParent?void 0:l.getOffsetParent(a.floating)),strategy:s}):i[f]);return{top:m.top-g.top+y.top,bottom:g.bottom-m.bottom+y.bottom,left:m.left-g.left+y.left,right:g.right-m.right+y.right}}let R=Math.min,k=Math.max;function T(e,t,r){return k(e,R(t,r))}let O={left:"right",right:"left",bottom:"top",top:"bottom"};function E(e){return e.replace(/left|right|bottom|top/g,e=>O[e])}let j={start:"end",end:"start"};function A(e){return e.replace(/start|end/g,e=>j[e])}function P(e){return e&&e.document&&e.location&&e.alert&&e.setInterval}function N(e){if(null==e)return window;if(!P(e)){let t=e.ownerDocument;return t&&t.defaultView||window}return e}function C(e){return N(e).getComputedStyle(e)}function L(e){return P(e)?"":e?(e.nodeName||"").toLowerCase():""}function $(){let e=navigator.userAgentData;return null!=e&&e.brands?e.brands.map(e=>e.brand+"/"+e.version).join(" "):navigator.userAgent}function D(e){return e instanceof N(e).HTMLElement}function W(e){return e instanceof N(e).Element}function F(e){return"undefined"!=typeof ShadowRoot&&(e instanceof N(e).ShadowRoot||e instanceof ShadowRoot)}function I(e){let{overflow:t,overflowX:r,overflowY:n,display:o}=C(e);return/auto|scroll|overlay|hidden/.test(t+n+r)&&!["inline","contents"].includes(o)}function H(e){return["table","td","th"].includes(L(e))}function B(e){let t=/firefox/i.test($()),r=C(e),n=r.backdropFilter||r.WebkitBackdropFilter;return"none"!==r.transform||"none"!==r.perspective||!!n&&"none"!==n||t&&"filter"===r.willChange||t&&!!r.filter&&"none"!==r.filter||["transform","perspective"].some(e=>r.willChange.includes(e))||["paint","layout","strict","content"].some(e=>{let t=r.contain;return null!=t&&t.includes(e)})}function M(){return!/^((?!chrome|android).)*safari/i.test($())}function U(e){return["html","body","#document"].includes(L(e))}let V=Math.min,z=Math.max,Y=Math.round;function q(e,t,r){var n,o,l,i;void 0===t&&(t=!1),void 0===r&&(r=!1);let a=e.getBoundingClientRect(),s=1,c=1;t&&D(e)&&(s=e.offsetWidth>0&&Y(a.width)/e.offsetWidth||1,c=e.offsetHeight>0&&Y(a.height)/e.offsetHeight||1);let u=W(e)?N(e):window,f=!M()&&r,p=(a.left+(f&&null!=(n=null==(o=u.visualViewport)?void 0:o.offsetLeft)?n:0))/s,d=(a.top+(f&&null!=(l=null==(i=u.visualViewport)?void 0:i.offsetTop)?l:0))/c,y=a.width/s,h=a.height/c;return{width:y,height:h,top:d,right:p+y,bottom:d+h,left:p,x:p,y:d}}function X(e){return((e instanceof N(e).Node?e.ownerDocument:e.document)||window.document).documentElement}function K(e){return W(e)?{scrollLeft:e.scrollLeft,scrollTop:e.scrollTop}:{scrollLeft:e.pageXOffset,scrollTop:e.pageYOffset}}function J(e){return q(X(e)).left+K(e).scrollLeft}function Z(e){if("html"===L(e))return e;let t=e.assignedSlot||e.parentNode||(F(e)?e.host:null)||X(e);return F(t)?t.host:t}function G(e){return D(e)&&"fixed"!==C(e).position?e.offsetParent:null}function Q(e){let t=N(e),r=G(e);for(;r&&H(r)&&"static"===C(r).position;)r=G(r);return r&&("html"===L(r)||"body"===L(r)&&"static"===C(r).position&&!B(r))?t:r||function(e){let t=Z(e);for(;D(t)&&!U(t);){if(B(t))return t;t=Z(t)}return null}(e)||t}function ee(e){if(D(e))return{width:e.offsetWidth,height:e.offsetHeight};let t=q(e);return{width:t.width,height:t.height}}function et(e,t,r){return"viewport"===t?S(function(e,t){let r=N(e),n=X(e),o=r.visualViewport,l=n.clientWidth,i=n.clientHeight,a=0,s=0;if(o){l=o.width,i=o.height;let c=M();(c||!c&&"fixed"===t)&&(a=o.offsetLeft,s=o.offsetTop)}return{width:l,height:i,x:a,y:s}}(e,r)):W(t)?function(e,t){let r=q(e,!1,"fixed"===t),n=r.top+e.clientTop,o=r.left+e.clientLeft;return{top:n,left:o,x:o,y:n,right:o+e.clientWidth,bottom:n+e.clientHeight,width:e.clientWidth,height:e.clientHeight}}(t,r):S(function(e){var t;let r=X(e),n=K(e),o=null==(t=e.ownerDocument)?void 0:t.body,l=z(r.scrollWidth,r.clientWidth,o?o.scrollWidth:0,o?o.clientWidth:0),i=z(r.scrollHeight,r.clientHeight,o?o.scrollHeight:0,o?o.clientHeight:0),a=-n.scrollLeft+J(e),s=-n.scrollTop;return"rtl"===C(o||r).direction&&(a+=z(r.clientWidth,o?o.clientWidth:0)-l),{width:l,height:i,x:a,y:s}}(X(e)))}let er={getClippingRect:function(e){let{element:t,boundary:r,rootBoundary:n,strategy:o}=e,l="clippingAncestors"===r?function(e){let t=(function e(t,r){var n;void 0===r&&(r=[]);let o=function e(t){let r=Z(t);return U(r)?t.ownerDocument.body:D(r)&&I(r)?r:e(r)}(t),l=o===(null==(n=t.ownerDocument)?void 0:n.body),i=N(o),a=l?[i].concat(i.visualViewport||[],I(o)?o:[]):o,s=r.concat(a);return l?s:s.concat(e(a))})(e).filter(e=>W(e)&&"body"!==L(e)),r=e,n=null;for(;W(r)&&!U(r);){let o=C(r);"static"===o.position&&n&&["absolute","fixed"].includes(n.position)&&!B(r)?t=t.filter(e=>e!==r):n=o,r=Z(r)}return t}(t):[].concat(r),i=[...l,n],a=i[0],s=i.reduce((e,r)=>{let n=et(t,r,o);return e.top=z(n.top,e.top),e.right=V(n.right,e.right),e.bottom=V(n.bottom,e.bottom),e.left=z(n.left,e.left),e},et(t,a,o));return{width:s.right-s.left,height:s.bottom-s.top,x:s.left,y:s.top}},convertOffsetParentRelativeRectToViewportRelativeRect:function(e){let{rect:t,offsetParent:r,strategy:n}=e,o=D(r),l=X(r);if(r===l)return t;let i={scrollLeft:0,scrollTop:0},a={x:0,y:0};if((o||!o&&"fixed"!==n)&&(("body"!==L(r)||I(l))&&(i=K(r)),D(r))){let s=q(r,!0);a.x=s.x+r.clientLeft,a.y=s.y+r.clientTop}return{...t,x:t.x-i.scrollLeft+a.x,y:t.y-i.scrollTop+a.y}},isElement:W,getDimensions:ee,getOffsetParent:Q,getDocumentElement:X,getElementRects(e){let{reference:t,floating:r,strategy:n}=e;return{reference:function(e,t,r){let n=D(t),o=X(t),l=q(e,n&&function(e){let t=q(e);return Y(t.width)!==e.offsetWidth||Y(t.height)!==e.offsetHeight}(t),"fixed"===r),i={scrollLeft:0,scrollTop:0},a={x:0,y:0};if(n||!n&&"fixed"!==r){if(("body"!==L(t)||I(o))&&(i=K(t)),D(t)){let s=q(t,!0);a.x=s.x+t.clientLeft,a.y=s.y+t.clientTop}else o&&(a.x=J(o))}return{x:l.left+i.scrollLeft-a.x,y:l.top+i.scrollTop-a.y,width:l.width,height:l.height}}(t,Q(r),n),floating:{...ee(r),x:0,y:0}}},getClientRects:e=>Array.from(e.getClientRects()),isRTL:e=>"rtl"===C(e).direction},en=(e,t,r)=>(async(e,t,r)=>{let{placement:n="bottom",strategy:o="absolute",middleware:l=[],platform:i}=r,a=l.filter(Boolean),s=await (null==i.isRTL?void 0:i.isRTL(t));if(null==i&&console.error("Floating UI: `platform` property was not passed to config. If you want to use Floating UI on the web, install @floating-ui/dom instead of the /core package. Otherwise, you can create your own `platform`: https://floating-ui.com/docs/platform"),a.filter(e=>{let{name:t}=e;return"autoPlacement"===t||"flip"===t}).length>1)throw Error("Floating UI: duplicate `flip` and/or `autoPlacement` middleware detected. This will lead to an infinite loop. Ensure only one of either has been passed to the `middleware` array.");e&&t||console.error("Floating UI: The reference and/or floating element was not defined when `computePosition()` was called. Ensure that both elements have been created and can be measured.");let c=await i.getElementRects({reference:e,floating:t,strategy:o}),{x:u,y:f}=b(c,n,s),p=n,d={},y=0;for(let h=0;h<a.length;h++){let{name:m,fn:g}=a[h],{x:v,y:w,data:x,reset:S}=await g({x:u,y:f,initialPlacement:n,placement:p,strategy:o,middlewareData:d,rects:c,platform:i,elements:{reference:e,floating:t}});u=null!=v?v:u,f=null!=w?w:f,d={...d,[m]:{...d[m],...x}},y>50&&console.warn("Floating UI: The middleware lifecycle appears to be running in an infinite loop. This is usually caused by a `reset` continually being returned without a break condition."),S&&y<=50&&(y++,"object"==typeof S&&(S.placement&&(p=S.placement),S.rects&&(c=!0===S.rects?await i.getElementRects({reference:e,floating:t,strategy:o}):S.rects),{x:u,y:f}=b(c,p,s)),h=-1)}return{x:u,y:f,placement:p,strategy:o,middlewareData:d}})(e,t,{platform:er,...r}),eo=async({elementReference:e=null,tooltipReference:t=null,tooltipArrowReference:r=null,place:n="top",offset:o=10,strategy:l="absolute"})=>{var i,a,s,c;if(!e||null===t)return{tooltipStyles:{},tooltipArrowStyles:{}};let u=[(void 0===(a=Number(o))&&(a=0),{name:"offset",options:a,async fn(e){let{x:t,y:r}=e,n=await async function(e,t){let{placement:r,platform:n,elements:o}=e,l=await (null==n.isRTL?void 0:n.isRTL(o.floating)),i=m(r),a=g(r),s="x"===v(r),c=["left","top"].includes(i)?-1:1,u=l&&s?-1:1,f="function"==typeof t?t(e):t,{mainAxis:p,crossAxis:d,alignmentAxis:y}="number"==typeof f?{mainAxis:f,crossAxis:0,alignmentAxis:null}:{mainAxis:0,crossAxis:0,alignmentAxis:null,...f};return a&&"number"==typeof y&&(d="end"===a?-1*y:y),s?{x:d*u,y:p*c}:{x:p*c,y:d*u}}(e,a);return{x:t+n.x,y:r+n.y,data:n}}}),(void 0===s&&(s={}),{name:"flip",options:s,async fn(e){var t,r,n,o;let{placement:l,middlewareData:i,rects:a,initialPlacement:c,platform:u,elements:f}=e,{mainAxis:p=!0,crossAxis:d=!0,fallbackPlacements:y,fallbackStrategy:h="bestFit",flipAlignment:b=!0,...x}=s,S=m(l),R=y||(S!==c&&b?function(e){let t=E(e);return[A(e),t,A(t)]}(c):[E(c)]),k=await _(e,x),T=[],O=(null==(t=i.flip)?void 0:t.overflows)||[];if(p&&T.push(k[S]),d){let{main:j,cross:P}=function(e,t,r){void 0===r&&(r=!1);let n=g(e),o=v(e),l=w(o),i="x"===o?n===(r?"end":"start")?"right":"left":"start"===n?"bottom":"top";return t.reference[l]>t.floating[l]&&(i=E(i)),{main:i,cross:E(i)}}(l,a,await (null==u.isRTL?void 0:u.isRTL(f.floating)));T.push(k[j],k[P])}if(O=[...O,{placement:l,overflows:T}],!T.every(e=>e<=0)){let N=(null!=(r=null==(n=i.flip)?void 0:n.index)?r:0)+1,C=[c,...R][N];if(C)return{data:{index:N,overflows:O},reset:{placement:C}};let L="bottom";switch(h){case"bestFit":{let $=null==(o=O.map(e=>[e,e.overflows.filter(e=>e>0).reduce((e,t)=>e+t,0)]).sort((e,t)=>e[1]-t[1])[0])?void 0:o[0].placement;$&&(L=$);break}case"initialPlacement":L=c}if(l!==L)return{reset:{placement:L}}}return{}}}),{name:"shift",options:i={padding:5},async fn(e){let{x:t,y:r,placement:n}=e,{mainAxis:o=!0,crossAxis:l=!1,limiter:a={fn(e){let{x:t,y:r}=e;return{x:t,y:r}}},...s}=i,c={x:t,y:r},u=await _(e,s),f=v(m(n)),p="x"===f?"y":"x",d=c[f],y=c[p];o&&(d=T(d+u["y"===f?"top":"left"],d,d-u["y"===f?"bottom":"right"])),l&&(y=T(y+u["y"===p?"top":"left"],y,y-u["y"===p?"bottom":"right"]));let h=a.fn({...e,[f]:d,[p]:y});return{...h,data:{x:h.x-t,y:h.y-r}}}}];return r?(u.push({name:"arrow",options:c={element:r,padding:5},async fn(e){let{element:t,padding:r=0}=null!=c?c:{},{x:n,y:o,placement:l,rects:i,platform:a}=e;if(null==t)return console.warn("Floating UI: No `element` was passed to the `arrow` middleware."),{};let s=x(r),u={x:n,y:o},f=v(l),p=g(l),d=w(f),y=await a.getDimensions(t),h="y"===f?"top":"left",m="y"===f?"bottom":"right",b=i.reference[d]+i.reference[f]-u[f]-i.floating[d],S=u[f]-i.reference[f],_=await (null==a.getOffsetParent?void 0:a.getOffsetParent(t)),R=_?"y"===f?_.clientHeight||0:_.clientWidth||0:0;0===R&&(R=i.floating[d]);let k=s[h],O=R-y[d]-s[m],E=R/2-y[d]/2+(b/2-S/2),j=T(k,E,O),A=("start"===p?s[h]:s[m])>0&&E!==j&&i.reference[d]<=i.floating[d];return{[f]:u[f]-(A?E<k?k-E:O-E:0),data:{[f]:j,centerOffset:E-j}}}}),en(e,t,{placement:n,strategy:l,middleware:u}).then(({x:e,y:t,placement:r,middlewareData:n})=>{var o,l;let i={left:`${e}px`,top:`${t}px`},{x:a,y:s}=null!==(o=n.arrow)&&void 0!==o?o:{x:0,y:0};return{tooltipStyles:i,tooltipArrowStyles:{left:null!=a?`${a}px`:"",top:null!=s?`${s}px`:"",right:"",bottom:"",[null!==(l=({top:"bottom",right:"left",bottom:"top",left:"right"})[r.split("-")[0]])&&void 0!==l?l:"bottom"]:"-4px"}}})):en(e,t,{placement:"bottom",strategy:l,middleware:u}).then(({x:e,y:t})=>({tooltipStyles:{left:`${e}px`,top:`${t}px`},tooltipArrowStyles:{}}))};var el={tooltip:"styles-module_tooltip__mnnfp",fixed:"styles-module_fixed__7ciUi",arrow:"styles-module_arrow__K0L3T","no-arrow":"styles-module_no-arrow__KcFZN",show:"styles-module_show__2NboJ",dark:"styles-module_dark__xNqje",light:"styles-module_light__Z6W-X",success:"styles-module_success__A2AKt",warning:"styles-module_warning__SCK0X",error:"styles-module_error__JvumD",info:"styles-module_info__BWdHW"};let ei=({id:e,className:t,classNameArrow:r,variant:l="dark",anchorId:i,place:a="top",offset:f=10,events:p=["hover"],positionStrategy:d="absolute",wrapper:h="div",children:m=null,delayShow:g=0,delayHide:v=0,float:w=!1,noArrow:b,style:x,position:S,isHtmlContent:_=!1,content:R,isOpen:k,setIsOpen:T})=>{let O=(0,n.useRef)(null),E=(0,n.useRef)(null),j=(0,n.useRef)(null),A=(0,n.useRef)(null),[P,N]=(0,n.useState)({}),[C,L]=(0,n.useState)({}),[$,D]=(0,n.useState)(!1),[W,F]=(0,n.useState)(!1),I=(0,n.useRef)(null),{anchorRefs:H,setActiveAnchor:B}=y()(e),[M,U]=(0,n.useState)({current:null}),V=e=>{T?T(e):void 0===k&&D(e)},z=()=>{A.current&&clearTimeout(A.current),A.current=setTimeout(()=>{V(!1)},v)},Y=e=>{var t;if(!e)return;g?(j.current&&clearTimeout(j.current),j.current=setTimeout(()=>{V(!0)},g)):V(!0);let r=null!==(t=e.currentTarget)&&void 0!==t?t:e.target;U(e=>e.current===r?e:{current:r}),B({current:r}),A.current&&clearTimeout(A.current)},q=({x:e,y:t})=>{F(!0),eo({place:a,offset:f,elementReference:{getBoundingClientRect:()=>({x:e,y:t,width:0,height:0,top:t,left:e,right:e,bottom:t})},tooltipReference:O.current,tooltipArrowReference:E.current,strategy:d}).then(e=>{F(!1),Object.keys(e.tooltipStyles).length&&N(e.tooltipStyles),Object.keys(e.tooltipArrowStyles).length&&L(e.tooltipArrowStyles)})},X=e=>{if(!e)return;let t={x:e.clientX,y:e.clientY};q(t),I.current=t},K=e=>{Y(e),v&&z()},J=e=>{var t;(null===(t=M.current)||void 0===t?void 0:t.contains(e.target))||D(!1)},Z=c(Y,50),G=c(()=>{v?z():V(!1),j.current&&clearTimeout(j.current)},50);(0,n.useEffect)(()=>{let e=new Set(H),t=document.querySelector(`[id='${i}']`);if(t&&(U(e=>e.current===t?e:{current:t}),e.add({current:t})),!e.size)return()=>null;let r=[];return p.find(e=>"click"===e)&&(window.addEventListener("click",J),r.push({event:"click",listener:K})),p.find(e=>"hover"===e)&&(r.push({event:"mouseenter",listener:Z},{event:"mouseleave",listener:G},{event:"focus",listener:Z},{event:"blur",listener:G}),w&&r.push({event:"mousemove",listener:X})),r.forEach(({event:t,listener:r})=>{e.forEach(e=>{var n;null===(n=e.current)||void 0===n||n.addEventListener(t,r)})}),()=>{window.removeEventListener("click",J),r.forEach(({event:t,listener:r})=>{e.forEach(e=>{var n;null===(n=e.current)||void 0===n||n.removeEventListener(t,r)})})}},[H,M,i,p,v,g]),(0,n.useEffect)(()=>{if(S)return q(S),()=>null;if(w)return I.current&&q(I.current),()=>null;let e=M.current;i&&(e=document.querySelector(`[id='${i}']`)),F(!0);let t=!0;return eo({place:a,offset:f,elementReference:e,tooltipReference:O.current,tooltipArrowReference:E.current,strategy:d}).then(e=>{t&&(F(!1),Object.keys(e.tooltipStyles).length&&N(e.tooltipStyles),Object.keys(e.tooltipArrowStyles).length&&L(e.tooltipArrowStyles))}),()=>{t=!1}},[$,k,i,M,R,a,f,d,S]),(0,n.useEffect)(()=>()=>{j.current&&clearTimeout(j.current),A.current&&clearTimeout(A.current)},[]);let Q=Boolean(R||m);return o.exports.jsxs(h,{id:e,role:"tooltip",className:s(el.tooltip,el[l],t,{[el.show]:Q&&!W&&(k||$),[el.fixed]:"fixed"===d}),style:{...x,...P},ref:O,children:[m||(_?o.exports.jsx(u,{content:R}):R),o.exports.jsx("div",{className:s(el.arrow,r,{[el["no-arrow"]]:b}),style:C,ref:E})]})},ea=({id:e,anchorId:t,content:r,html:l,className:i,classNameArrow:a,variant:s="dark",place:c="top",offset:u=10,wrapper:f="div",children:p=null,events:d=["hover"],positionStrategy:h="absolute",delayShow:m=0,delayHide:g=0,float:v=!1,noArrow:w,style:b,position:x,isOpen:S,setIsOpen:_})=>{let[R,k]=(0,n.useState)(r||l),[T,O]=(0,n.useState)(c),[E,j]=(0,n.useState)(s),[A,P]=(0,n.useState)(u),[N,C]=(0,n.useState)(m),[L,$]=(0,n.useState)(g),[D,W]=(0,n.useState)(v),[F,I]=(0,n.useState)(f),[H,B]=(0,n.useState)(d),[M,U]=(0,n.useState)(h),[V,z]=(0,n.useState)(Boolean(l)),{anchorRefs:Y,activeAnchor:q}=y()(e),X=e=>null==e?void 0:e.getAttributeNames().reduce((t,r)=>{var n;return r.startsWith("data-tooltip-")&&(t[r.replace(/^data-tooltip-/,"")]=null!==(n=null==e?void 0:e.getAttribute(r))&&void 0!==n?n:null),t},{}),K=e=>{let t={place(e){O(null!=e?e:c)},content(e){k(null!=e?e:r)},html(e){var t;z(Boolean(null!=e?e:l)),k(null!==(t=null!=e?e:l)&&void 0!==t?t:r)},variant(e){j(null!=e?e:s)},offset(e){P(null===e?u:Number(e))},wrapper(e){I(null!=e?e:"div")},events(e){let t=null==e?void 0:e.split(" ");B(null!=t?t:d)},"position-strategy"(e){U(null!=e?e:h)},"delay-show"(e){C(null===e?m:Number(e))},"delay-hide"(e){$(null===e?g:Number(e))},float(e){W(null===e?v:Boolean(e))}};Object.values(t).forEach(e=>e(null)),Object.entries(e).forEach(([e,r])=>{var n;null===(n=t[e])||void 0===n||n.call(t,r)})};(0,n.useEffect)(()=>{z(!1),k(r),l&&(z(!0),k(l))},[r,l]),(0,n.useEffect)(()=>{var e;let r=new Set(Y),n=document.querySelector(`[id='${t}']`);if(n&&r.add({current:n}),!r.size)return()=>null;let o=new MutationObserver(e=>{e.forEach(e=>{var t;if(!q.current||"attributes"!==e.type||!(null===(t=e.attributeName)||void 0===t?void 0:t.startsWith("data-tooltip-")))return;let r=X(q.current);K(r)})}),l=null!==(e=q.current)&&void 0!==e?e:n;if(l){let i=X(l);K(i),o.observe(l,{attributes:!0,childList:!1,subtree:!1})}return()=>{o.disconnect()}},[Y,q,t]);let J={id:e,anchorId:t,className:i,classNameArrow:a,content:R,isHtmlContent:V,place:T,variant:E,offset:A,wrapper:F,events:H,positionStrategy:M,delayShow:N,delayHide:L,float:D,noArrow:w,style:b,position:x,isOpen:S,setIsOpen:_};return p?o.exports.jsx(ei,{...J,children:p}):o.exports.jsx(ei,{...J})}}}]);