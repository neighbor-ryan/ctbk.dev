"use strict";(self.webpackChunk_N_E=self.webpackChunk_N_E||[]).push([[831],{25831:function(e,t,r){r.r(t),r.d(t,{Tooltip:function(){return el},TooltipProvider:function(){return d},TooltipWrapper:function(){return h}});var n=r(67294),o={exports:{}},l={};!function(){var e=Symbol.for("react.element"),t=Symbol.for("react.portal"),r=Symbol.for("react.fragment"),o=Symbol.for("react.strict_mode"),i=Symbol.for("react.profiler"),a=Symbol.for("react.provider"),s=Symbol.for("react.context"),c=Symbol.for("react.forward_ref"),u=Symbol.for("react.suspense"),f=Symbol.for("react.suspense_list"),p=Symbol.for("react.memo"),d=Symbol.for("react.lazy"),y=Symbol.for("react.offscreen"),h=Symbol.iterator,m=n.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED;function g(e){for(var t,r,n,o,l=arguments.length,i=Array(l>1?l-1:0),a=1;a<l;a++)i[a-1]=arguments[a];t=e,r=i,""!==(n=m.ReactDebugCurrentFrame.getStackAddendum())&&(t+="%s",r=r.concat([n])),(o=r.map(function(e){return String(e)})).unshift("Warning: "+t),Function.prototype.apply.call(console.error,console,o)}function v(e){return e.displayName||"Context"}function w(e){if(null==e)return null;if("number"==typeof e.tag&&g("Received an unexpected object in getComponentNameFromType(). This is likely a bug in React. Please file an issue."),"function"==typeof e)return e.displayName||e.name||null;if("string"==typeof e)return e;switch(e){case r:return"Fragment";case t:return"Portal";case i:return"Profiler";case o:return"StrictMode";case u:return"Suspense";case f:return"SuspenseList"}if("object"==typeof e)switch(e.$$typeof){case s:return v(e)+".Consumer";case a:return v(e._context)+".Provider";case c:return function(e,t,r){var n=e.displayName;if(n)return n;var o=t.displayName||t.name||"";return""!==o?r+"("+o+")":r}(e,e.render,"ForwardRef");case p:var n=e.displayName||null;return null!==n?n:w(e.type)||"Memo";case d:var l=e._payload,y=e._init;try{return w(y(l))}catch(e){}}return null}b=Symbol.for("react.module.reference");var b,x,S,_,R,k,T,O,E=Object.assign,j=0;function A(){}A.__reactDisabledLog=!0;var P,N=m.ReactCurrentDispatcher;function C(e,t,r){if(void 0===P)try{throw Error()}catch(e){var n=e.stack.trim().match(/\n( *(at )?)/);P=n&&n[1]||""}return"\n"+P+e}var L,$=!1;function D(e,t){if(!e||$)return"";var r,n=L.get(e);if(void 0!==n)return n;$=!0;var o,l=Error.prepareStackTrace;Error.prepareStackTrace=void 0,o=N.current,N.current=null,function(){if(0===j){x=console.log,S=console.info,_=console.warn,R=console.error,k=console.group,T=console.groupCollapsed,O=console.groupEnd;var e={configurable:!0,enumerable:!0,value:A,writable:!0};Object.defineProperties(console,{info:e,log:e,warn:e,error:e,group:e,groupCollapsed:e,groupEnd:e})}j++}();try{if(t){var i=function(){throw Error()};if(Object.defineProperty(i.prototype,"props",{set:function(){throw Error()}}),"object"==typeof Reflect&&Reflect.construct){try{Reflect.construct(i,[])}catch(e){r=e}Reflect.construct(e,[],i)}else{try{i.call()}catch(e){r=e}e.call(i.prototype)}}else{try{throw Error()}catch(e){r=e}e()}}catch(t){if(t&&r&&"string"==typeof t.stack){for(var a=t.stack.split("\n"),s=r.stack.split("\n"),c=a.length-1,u=s.length-1;c>=1&&u>=0&&a[c]!==s[u];)u--;for(;c>=1&&u>=0;c--,u--)if(a[c]!==s[u]){if(1!==c||1!==u)do if(c--,--u<0||a[c]!==s[u]){var f="\n"+a[c].replace(" at new "," at ");return e.displayName&&f.includes("<anonymous>")&&(f=f.replace("<anonymous>",e.displayName)),"function"==typeof e&&L.set(e,f),f}while(c>=1&&u>=0);break}}}finally{$=!1,N.current=o,function(){if(0==--j){var e={configurable:!0,enumerable:!0,writable:!0};Object.defineProperties(console,{log:E({},e,{value:x}),info:E({},e,{value:S}),warn:E({},e,{value:_}),error:E({},e,{value:R}),group:E({},e,{value:k}),groupCollapsed:E({},e,{value:T}),groupEnd:E({},e,{value:O})})}j<0&&g("disabledDepth fell below zero. This is a bug in React. Please file an issue.")}(),Error.prepareStackTrace=l}var p=e?e.displayName||e.name:"",d=p?C(p):"";return"function"==typeof e&&L.set(e,d),d}function W(e,t,r){if(null==e)return"";if("function"==typeof e)return D(e,!(!(n=e.prototype)||!n.isReactComponent));if("string"==typeof e)return C(e);switch(e){case u:return C("Suspense");case f:return C("SuspenseList")}if("object"==typeof e)switch(e.$$typeof){case c:return D(e.render,!1);case p:return W(e.type,t,r);case d:var n,o=e._payload,l=e._init;try{return W(l(o),t,r)}catch(e){}}return""}L=new("function"==typeof WeakMap?WeakMap:Map);var F=Object.prototype.hasOwnProperty,I={},H=m.ReactDebugCurrentFrame;function B(e){if(e){var t=e._owner,r=W(e.type,e._source,t?t.type:null);H.setExtraStackFrame(r)}else H.setExtraStackFrame(null)}var M=Array.isArray;function U(e){if(function(e){try{return!1}catch(e){return!0}}(0))return g("The provided key is an unsupported type %s. This value must be coerced to a string before before using it here.","function"==typeof Symbol&&Symbol.toStringTag&&e[Symbol.toStringTag]||e.constructor.name||"Object"),""+e}var V,z,Y,q=m.ReactCurrentOwner,X={key:!0,ref:!0,__self:!0,__source:!0};Y={};var K,J=m.ReactCurrentOwner,Z=m.ReactDebugCurrentFrame;function G(e){if(e){var t=e._owner,r=W(e.type,e._source,t?t.type:null);Z.setExtraStackFrame(r)}else Z.setExtraStackFrame(null)}function Q(t){return"object"==typeof t&&null!==t&&t.$$typeof===e}function ee(){if(J.current){var e=w(J.current.type);if(e)return"\n\nCheck the render method of `"+e+"`."}return""}K=!1;var et={};function er(e,t){if(e._store&&!e._store.validated&&null==e.key){e._store.validated=!0;var r=function(e){var t=ee();if(!t){var r="string"==typeof e?e:e.displayName||e.name;r&&(t="\n\nCheck the top-level render call using <"+r+">.")}return t}(t);if(!et[r]){et[r]=!0;var n="";e&&e._owner&&e._owner!==J.current&&(n=" It was passed a child from "+w(e._owner.type)+"."),G(e),g('Each child in a list should have a unique "key" prop.%s%s See https://reactjs.org/link/warning-keys for more information.',r,n),G(null)}}}function en(e,t){if("object"==typeof e){if(M(e))for(var r=0;r<e.length;r++){var n=e[r];Q(n)&&er(n,t)}else if(Q(e))e._store&&(e._store.validated=!0);else if(e){var o=function(e){if(null===e||"object"!=typeof e)return null;var t=h&&e[h]||e["@@iterator"];return"function"==typeof t?t:null}(e);if("function"==typeof o&&o!==e.entries)for(var l,i=o.call(e);!(l=i.next()).done;)Q(l.value)&&er(l.value,t)}}}function eo(t,n,l,h,m,v){var x="string"==typeof t||"function"==typeof t||t===r||t===i||t===o||t===u||t===f||t===y||"object"==typeof t&&null!==t&&(t.$$typeof===d||t.$$typeof===p||t.$$typeof===a||t.$$typeof===s||t.$$typeof===c||t.$$typeof===b||void 0!==t.getModuleId);if(!x){var S,_="";(void 0===t||"object"==typeof t&&null!==t&&0===Object.keys(t).length)&&(_+=" You likely forgot to export your component from the file it's defined in, or you might have mixed up default and named imports."),_+=(void 0!==m?"\n\nCheck your code at "+m.fileName.replace(/^.*[\\\/]/,"")+":"+m.lineNumber+".":"")||ee(),null===t?S="null":M(t)?S="array":void 0!==t&&t.$$typeof===e?(S="<"+(w(t.type)||"Unknown")+" />",_=" Did you accidentally export a JSX literal instead of a component?"):S=typeof t,g("React.jsx: type is invalid -- expected a string (for built-in components) or a class/function (for composite components) but got: %s.%s",S,_)}var R=function(t,r,n,o,l){var i,a,s={},c=null,u=null;for(a in void 0!==n&&(U(n),c=""+n),function(e){if(F.call(e,"key")){var t=Object.getOwnPropertyDescriptor(e,"key").get;if(t&&t.isReactWarning)return!1}return void 0!==e.key}(r)&&(U(r.key),c=""+r.key),function(e){if(F.call(e,"ref")){var t=Object.getOwnPropertyDescriptor(e,"ref").get;if(t&&t.isReactWarning)return!1}return void 0!==e.ref}(r)&&(u=r.ref,function(e,t){if("string"==typeof e.ref&&q.current&&t&&q.current.stateNode!==t){var r=w(q.current.type);Y[r]||(g('Component "%s" contains the string ref "%s". Support for string refs will be removed in a future major release. This case cannot be automatically converted to an arrow function. We ask you to manually fix this case by using useRef() or createRef() instead. Learn more about using refs safely here: https://reactjs.org/link/strict-mode-string-ref',w(q.current.type),e.ref),Y[r]=!0)}}(r,l)),r)F.call(r,a)&&!X.hasOwnProperty(a)&&(s[a]=r[a]);if(t&&t.defaultProps){var f=t.defaultProps;for(a in f)void 0===s[a]&&(s[a]=f[a])}if(c||u){var p,d,y="function"==typeof t?t.displayName||t.name||"Unknown":t;c&&((p=function(){V||(V=!0,g("%s: `key` is not a prop. Trying to access it will result in `undefined` being returned. If you need to access the same value within the child component, you should pass it as a different prop. (https://reactjs.org/link/special-props)",y))}).isReactWarning=!0,Object.defineProperty(s,"key",{get:p,configurable:!0})),u&&((d=function(){z||(z=!0,g("%s: `ref` is not a prop. Trying to access it will result in `undefined` being returned. If you need to access the same value within the child component, you should pass it as a different prop. (https://reactjs.org/link/special-props)",y))}).isReactWarning=!0,Object.defineProperty(s,"ref",{get:d,configurable:!0}))}return Object.defineProperty((i={$$typeof:e,type:t,key:c,ref:u,props:s,_owner:q.current,_store:{}})._store,"validated",{configurable:!1,enumerable:!1,writable:!0,value:!1}),Object.defineProperty(i,"_self",{configurable:!1,enumerable:!1,writable:!1,value:l}),Object.defineProperty(i,"_source",{configurable:!1,enumerable:!1,writable:!1,value:o}),Object.freeze&&(Object.freeze(i.props),Object.freeze(i)),i}(t,n,l,m,v);if(null==R)return R;if(x){var k=n.children;if(void 0!==k){if(h){if(M(k)){for(var T=0;T<k.length;T++)en(k[T],t);Object.freeze&&Object.freeze(k)}else g("React.jsx: Static children should always be an array. You are likely explicitly calling React.jsxs or React.jsxDEV. Use the Babel transform instead.")}else en(k,t)}}return t===r?function(e){for(var t=Object.keys(e.props),r=0;r<t.length;r++){var n=t[r];if("children"!==n&&"key"!==n){G(e),g("Invalid prop `%s` supplied to `React.Fragment`. React.Fragment can only have `key` and `children` props.",n),G(null);break}}null!==e.ref&&(G(e),g("Invalid attribute `ref` supplied to `React.Fragment`."),G(null))}(R):function(e){var t,r=e.type;if(null!=r&&"string"!=typeof r){if("function"==typeof r)t=r.propTypes;else{if("object"!=typeof r||r.$$typeof!==c&&r.$$typeof!==p)return;t=r.propTypes}if(t){var n=w(r);!function(e,t,r,n,o){var l=Function.call.bind(F);for(var i in e)if(l(e,i)){var a=void 0;try{if("function"!=typeof e[i]){var s=Error((n||"React class")+": "+r+" type `"+i+"` is invalid; it must be a function, usually from the `prop-types` package, but received `"+typeof e[i]+"`.This often happens because of typos such as `PropTypes.function` instead of `PropTypes.func`.");throw s.name="Invariant Violation",s}a=e[i](t,i,n,r,null,"SECRET_DO_NOT_PASS_THIS_OR_YOU_WILL_BE_FIRED")}catch(e){a=e}!a||a instanceof Error||(B(o),g("%s: type specification of %s `%s` is invalid; the type checker function must return `null` or an `Error` but returned a %s. You may have forgotten to pass an argument to the type checker creator (arrayOf, instanceOf, objectOf, oneOf, oneOfType, and shape all require an argument).",n||"React class",r,i,typeof a),B(null)),a instanceof Error&&!(a.message in I)&&(I[a.message]=!0,B(o),g("Failed %s type: %s",r,a.message),B(null))}}(t,e.props,"prop",n,e)}else void 0===r.PropTypes||K||(K=!0,g("Component %s declared `PropTypes` instead of `propTypes`. Did you misspell the property assignment?",w(r)||"Unknown"));"function"!=typeof r.getDefaultProps||r.getDefaultProps.isReactClassApproved||g("getDefaultProps is only used on classic React.createClass definitions. Use a static property named `defaultProps` instead.")}}(R),R}l.Fragment=r,l.jsx=function(e,t,r){return eo(e,t,r,!1)},l.jsxs=function(e,t,r){return eo(e,t,r,!0)}}(),o.exports=l;var i,a={exports:{}};/*!
	Copyright (c) 2018 Jed Watson.
	Licensed under the MIT License (MIT), see
	http://jedwatson.github.io/classnames
*/i=a,function(){var e={}.hasOwnProperty;function t(){for(var r=[],n=0;n<arguments.length;n++){var o=arguments[n];if(o){var l=typeof o;if("string"===l||"number"===l)r.push(o);else if(Array.isArray(o)){if(o.length){var i=t.apply(null,o);i&&r.push(i)}}else if("object"===l){if(o.toString!==Object.prototype.toString&&!o.toString.toString().includes("[native code]")){r.push(o.toString());continue}for(var a in o)e.call(o,a)&&o[a]&&r.push(a)}}}return r.join(" ")}i.exports?(t.default=t,i.exports=t):window.classNames=t}();var s=a.exports;let c=(e,t,r)=>{let n=null;return function(...o){n&&clearTimeout(n),n=setTimeout(()=>{n=null,r||e.apply(this,o)},t)}},u=({content:e})=>o.exports.jsx("span",{dangerouslySetInnerHTML:{__html:e}}),f={anchorRefs:new Set,activeAnchor:{current:null},attach:()=>{},detach:()=>{},setActiveAnchor:()=>{}},p=(0,n.createContext)(Object.assign(()=>f,f)),d=({children:e})=>{let t=(0,n.useId)(),[r,l]=(0,n.useState)({[t]:new Set}),[i,a]=(0,n.useState)({[t]:{current:null}}),s=(e,...t)=>{l(r=>{var n;let o=null!==(n=r[e])&&void 0!==n?n:new Set;return t.forEach(e=>o.add(e)),{...r,[e]:new Set(o)}})},c=(e,...t)=>{l(r=>{let n=r[e];return n?(t.forEach(e=>n.delete(e)),{...r}):r})},u=(0,n.useCallback)(e=>{var n,o;return{anchorRefs:null!==(n=r[null!=e?e:t])&&void 0!==n?n:new Set,activeAnchor:null!==(o=i[null!=e?e:t])&&void 0!==o?o:{current:null},attach:(...r)=>s(null!=e?e:t,...r),detach:(...r)=>c(null!=e?e:t,...r),setActiveAnchor:r=>{var n;return n=null!=e?e:t,void a(e=>{var t;return(null===(t=e[n])||void 0===t?void 0:t.current)===r.current?e:{...e,[n]:r}})}}},[t,r,i,s,c]),f=(0,n.useMemo)(()=>{let e=u(t);return Object.assign(e=>u(e),e)},[u]);return o.exports.jsx(p.Provider,{value:f,children:e})};function y(){return(0,n.useContext)(p)}let h=({tooltipId:e,children:t,place:r,content:l,html:i,variant:a,offset:s,wrapper:c,events:u,positionStrategy:f,delayShow:p,delayHide:d})=>{let{attach:h,detach:m}=y()(e),g=(0,n.useRef)(null);return(0,n.useEffect)(()=>(h(g),()=>{m(g)}),[]),o.exports.jsx("span",{ref:g,"data-tooltip-place":r,"data-tooltip-content":l,"data-tooltip-html":i,"data-tooltip-variant":a,"data-tooltip-offset":s,"data-tooltip-wrapper":c,"data-tooltip-events":u,"data-tooltip-position-strategy":f,"data-tooltip-delay-show":p,"data-tooltip-delay-hide":d,children:t})};function m(e){return e.split("-")[0]}function g(e){return e.split("-")[1]}function v(e){return["top","bottom"].includes(m(e))?"x":"y"}function w(e){return"y"===e?"height":"width"}function b(e,t,r){let n,{reference:o,floating:l}=e,i=o.x+o.width/2-l.width/2,a=o.y+o.height/2-l.height/2,s=v(t),c=w(s),u=o[c]/2-l[c]/2,f="x"===s;switch(m(t)){case"top":n={x:i,y:o.y-l.height};break;case"bottom":n={x:i,y:o.y+o.height};break;case"right":n={x:o.x+o.width,y:a};break;case"left":n={x:o.x-l.width,y:a};break;default:n={x:o.x,y:o.y}}switch(g(t)){case"start":n[s]-=u*(r&&f?-1:1);break;case"end":n[s]+=u*(r&&f?-1:1)}return n}function x(e){return"number"!=typeof e?{top:0,right:0,bottom:0,left:0,...e}:{top:e,right:e,bottom:e,left:e}}function S(e){return{...e,top:e.y,left:e.x,right:e.x+e.width,bottom:e.y+e.height}}async function _(e,t){var r;void 0===t&&(t={});let{x:n,y:o,platform:l,rects:i,elements:a,strategy:s}=e,{boundary:c="clippingAncestors",rootBoundary:u="viewport",elementContext:f="floating",altBoundary:p=!1,padding:d=0}=t,y=x(d),h=a[p?"floating"===f?"reference":"floating":f],m=S(await l.getClippingRect({element:null==(r=await (null==l.isElement?void 0:l.isElement(h)))||r?h:h.contextElement||await (null==l.getDocumentElement?void 0:l.getDocumentElement(a.floating)),boundary:c,rootBoundary:u,strategy:s})),g=S(l.convertOffsetParentRelativeRectToViewportRelativeRect?await l.convertOffsetParentRelativeRectToViewportRelativeRect({rect:"floating"===f?{...i.floating,x:n,y:o}:i.reference,offsetParent:await (null==l.getOffsetParent?void 0:l.getOffsetParent(a.floating)),strategy:s}):i[f]);return{top:m.top-g.top+y.top,bottom:g.bottom-m.bottom+y.bottom,left:m.left-g.left+y.left,right:g.right-m.right+y.right}}let R=Math.min,k=Math.max,T={left:"right",right:"left",bottom:"top",top:"bottom"};function O(e){return e.replace(/left|right|bottom|top/g,e=>T[e])}let E={start:"end",end:"start"};function j(e){return e.replace(/start|end/g,e=>E[e])}function A(e){return e&&e.document&&e.location&&e.alert&&e.setInterval}function P(e){if(null==e)return window;if(!A(e)){let t=e.ownerDocument;return t&&t.defaultView||window}return e}function N(e){return P(e).getComputedStyle(e)}function C(e){return A(e)?"":e?(e.nodeName||"").toLowerCase():""}function L(){let e=navigator.userAgentData;return null!=e&&e.brands?e.brands.map(e=>e.brand+"/"+e.version).join(" "):navigator.userAgent}function $(e){return e instanceof P(e).HTMLElement}function D(e){return e instanceof P(e).Element}function W(e){return"undefined"!=typeof ShadowRoot&&(e instanceof P(e).ShadowRoot||e instanceof ShadowRoot)}function F(e){let{overflow:t,overflowX:r,overflowY:n,display:o}=N(e);return/auto|scroll|overlay|hidden/.test(t+n+r)&&!["inline","contents"].includes(o)}function I(e){let t=/firefox/i.test(L()),r=N(e),n=r.backdropFilter||r.WebkitBackdropFilter;return"none"!==r.transform||"none"!==r.perspective||!!n&&"none"!==n||t&&"filter"===r.willChange||t&&!!r.filter&&"none"!==r.filter||["transform","perspective"].some(e=>r.willChange.includes(e))||["paint","layout","strict","content"].some(e=>{let t=r.contain;return null!=t&&t.includes(e)})}function H(){return!/^((?!chrome|android).)*safari/i.test(L())}function B(e){return["html","body","#document"].includes(C(e))}let M=Math.min,U=Math.max,V=Math.round;function z(e,t,r){var n,o,l,i;void 0===t&&(t=!1),void 0===r&&(r=!1);let a=e.getBoundingClientRect(),s=1,c=1;t&&$(e)&&(s=e.offsetWidth>0&&V(a.width)/e.offsetWidth||1,c=e.offsetHeight>0&&V(a.height)/e.offsetHeight||1);let u=D(e)?P(e):window,f=!H()&&r,p=(a.left+(f&&null!=(n=null==(o=u.visualViewport)?void 0:o.offsetLeft)?n:0))/s,d=(a.top+(f&&null!=(l=null==(i=u.visualViewport)?void 0:i.offsetTop)?l:0))/c,y=a.width/s,h=a.height/c;return{width:y,height:h,top:d,right:p+y,bottom:d+h,left:p,x:p,y:d}}function Y(e){return((e instanceof P(e).Node?e.ownerDocument:e.document)||window.document).documentElement}function q(e){return D(e)?{scrollLeft:e.scrollLeft,scrollTop:e.scrollTop}:{scrollLeft:e.pageXOffset,scrollTop:e.pageYOffset}}function X(e){return z(Y(e)).left+q(e).scrollLeft}function K(e){if("html"===C(e))return e;let t=e.assignedSlot||e.parentNode||(W(e)?e.host:null)||Y(e);return W(t)?t.host:t}function J(e){return $(e)&&"fixed"!==N(e).position?e.offsetParent:null}function Z(e){let t=P(e),r=J(e);for(;r&&["table","td","th"].includes(C(r))&&"static"===N(r).position;)r=J(r);return r&&("html"===C(r)||"body"===C(r)&&"static"===N(r).position&&!I(r))?t:r||function(e){let t=K(e);for(;$(t)&&!B(t);){if(I(t))return t;t=K(t)}return null}(e)||t}function G(e){if($(e))return{width:e.offsetWidth,height:e.offsetHeight};let t=z(e);return{width:t.width,height:t.height}}function Q(e,t,r){return"viewport"===t?S(function(e,t){let r=P(e),n=Y(e),o=r.visualViewport,l=n.clientWidth,i=n.clientHeight,a=0,s=0;if(o){l=o.width,i=o.height;let e=H();(e||!e&&"fixed"===t)&&(a=o.offsetLeft,s=o.offsetTop)}return{width:l,height:i,x:a,y:s}}(e,r)):D(t)?function(e,t){let r=z(e,!1,"fixed"===t),n=r.top+e.clientTop,o=r.left+e.clientLeft;return{top:n,left:o,x:o,y:n,right:o+e.clientWidth,bottom:n+e.clientHeight,width:e.clientWidth,height:e.clientHeight}}(t,r):S(function(e){var t;let r=Y(e),n=q(e),o=null==(t=e.ownerDocument)?void 0:t.body,l=U(r.scrollWidth,r.clientWidth,o?o.scrollWidth:0,o?o.clientWidth:0),i=U(r.scrollHeight,r.clientHeight,o?o.scrollHeight:0,o?o.clientHeight:0),a=-n.scrollLeft+X(e),s=-n.scrollTop;return"rtl"===N(o||r).direction&&(a+=U(r.clientWidth,o?o.clientWidth:0)-l),{width:l,height:i,x:a,y:s}}(Y(e)))}let ee={getClippingRect:function(e){let{element:t,boundary:r,rootBoundary:n,strategy:o}=e,l="clippingAncestors"===r?function(e){let t=(function e(t,r){var n;void 0===r&&(r=[]);let o=function e(t){let r=K(t);return B(r)?t.ownerDocument.body:$(r)&&F(r)?r:e(r)}(t),l=o===(null==(n=t.ownerDocument)?void 0:n.body),i=P(o),a=l?[i].concat(i.visualViewport||[],F(o)?o:[]):o,s=r.concat(a);return l?s:s.concat(e(a))})(e).filter(e=>D(e)&&"body"!==C(e)),r=e,n=null;for(;D(r)&&!B(r);){let e=N(r);"static"===e.position&&n&&["absolute","fixed"].includes(n.position)&&!I(r)?t=t.filter(e=>e!==r):n=e,r=K(r)}return t}(t):[].concat(r),i=[...l,n],a=i[0],s=i.reduce((e,r)=>{let n=Q(t,r,o);return e.top=U(n.top,e.top),e.right=M(n.right,e.right),e.bottom=M(n.bottom,e.bottom),e.left=U(n.left,e.left),e},Q(t,a,o));return{width:s.right-s.left,height:s.bottom-s.top,x:s.left,y:s.top}},convertOffsetParentRelativeRectToViewportRelativeRect:function(e){let{rect:t,offsetParent:r,strategy:n}=e,o=$(r),l=Y(r);if(r===l)return t;let i={scrollLeft:0,scrollTop:0},a={x:0,y:0};if((o||!o&&"fixed"!==n)&&(("body"!==C(r)||F(l))&&(i=q(r)),$(r))){let e=z(r,!0);a.x=e.x+r.clientLeft,a.y=e.y+r.clientTop}return{...t,x:t.x-i.scrollLeft+a.x,y:t.y-i.scrollTop+a.y}},isElement:D,getDimensions:G,getOffsetParent:Z,getDocumentElement:Y,getElementRects:e=>{let{reference:t,floating:r,strategy:n}=e;return{reference:function(e,t,r){let n=$(t),o=Y(t),l=z(e,n&&function(e){let t=z(e);return V(t.width)!==e.offsetWidth||V(t.height)!==e.offsetHeight}(t),"fixed"===r),i={scrollLeft:0,scrollTop:0},a={x:0,y:0};if(n||!n&&"fixed"!==r){if(("body"!==C(t)||F(o))&&(i=q(t)),$(t)){let e=z(t,!0);a.x=e.x+t.clientLeft,a.y=e.y+t.clientTop}else o&&(a.x=X(o))}return{x:l.left+i.scrollLeft-a.x,y:l.top+i.scrollTop-a.y,width:l.width,height:l.height}}(t,Z(r),n),floating:{...G(r),x:0,y:0}}},getClientRects:e=>Array.from(e.getClientRects()),isRTL:e=>"rtl"===N(e).direction},et=(e,t,r)=>(async(e,t,r)=>{let{placement:n="bottom",strategy:o="absolute",middleware:l=[],platform:i}=r,a=l.filter(Boolean),s=await (null==i.isRTL?void 0:i.isRTL(t));if(null==i&&console.error("Floating UI: `platform` property was not passed to config. If you want to use Floating UI on the web, install @floating-ui/dom instead of the /core package. Otherwise, you can create your own `platform`: https://floating-ui.com/docs/platform"),a.filter(e=>{let{name:t}=e;return"autoPlacement"===t||"flip"===t}).length>1)throw Error("Floating UI: duplicate `flip` and/or `autoPlacement` middleware detected. This will lead to an infinite loop. Ensure only one of either has been passed to the `middleware` array.");e&&t||console.error("Floating UI: The reference and/or floating element was not defined when `computePosition()` was called. Ensure that both elements have been created and can be measured.");let c=await i.getElementRects({reference:e,floating:t,strategy:o}),{x:u,y:f}=b(c,n,s),p=n,d={},y=0;for(let r=0;r<a.length;r++){let{name:l,fn:h}=a[r],{x:m,y:g,data:v,reset:w}=await h({x:u,y:f,initialPlacement:n,placement:p,strategy:o,middlewareData:d,rects:c,platform:i,elements:{reference:e,floating:t}});u=null!=m?m:u,f=null!=g?g:f,d={...d,[l]:{...d[l],...v}},y>50&&console.warn("Floating UI: The middleware lifecycle appears to be running in an infinite loop. This is usually caused by a `reset` continually being returned without a break condition."),w&&y<=50&&(y++,"object"==typeof w&&(w.placement&&(p=w.placement),w.rects&&(c=!0===w.rects?await i.getElementRects({reference:e,floating:t,strategy:o}):w.rects),{x:u,y:f}=b(c,p,s)),r=-1)}return{x:u,y:f,placement:p,strategy:o,middlewareData:d}})(e,t,{platform:ee,...r}),er=async({elementReference:e=null,tooltipReference:t=null,tooltipArrowReference:r=null,place:n="top",offset:o=10,strategy:l="absolute"})=>{var i,a,s,c;if(!e||null===t)return{tooltipStyles:{},tooltipArrowStyles:{}};let u=[(void 0===(a=Number(o))&&(a=0),{name:"offset",options:a,async fn(e){let{x:t,y:r}=e,n=await async function(e,t){let{placement:r,platform:n,elements:o}=e,l=await (null==n.isRTL?void 0:n.isRTL(o.floating)),i=m(r),a=g(r),s="x"===v(r),c=["left","top"].includes(i)?-1:1,u=l&&s?-1:1,f="function"==typeof t?t(e):t,{mainAxis:p,crossAxis:d,alignmentAxis:y}="number"==typeof f?{mainAxis:f,crossAxis:0,alignmentAxis:null}:{mainAxis:0,crossAxis:0,alignmentAxis:null,...f};return a&&"number"==typeof y&&(d="end"===a?-1*y:y),s?{x:d*u,y:p*c}:{x:p*c,y:d*u}}(e,a);return{x:t+n.x,y:r+n.y,data:n}}}),(void 0===s&&(s={}),{name:"flip",options:s,async fn(e){var t,r,n,o;let{placement:l,middlewareData:i,rects:a,initialPlacement:c,platform:u,elements:f}=e,{mainAxis:p=!0,crossAxis:d=!0,fallbackPlacements:y,fallbackStrategy:h="bestFit",flipAlignment:b=!0,...x}=s,S=m(l),R=y||(S!==c&&b?function(e){let t=O(e);return[j(e),t,j(t)]}(c):[O(c)]),k=[c,...R],T=await _(e,x),E=[],A=(null==(t=i.flip)?void 0:t.overflows)||[];if(p&&E.push(T[S]),d){let{main:e,cross:t}=function(e,t,r){void 0===r&&(r=!1);let n=g(e),o=v(e),l=w(o),i="x"===o?n===(r?"end":"start")?"right":"left":"start"===n?"bottom":"top";return t.reference[l]>t.floating[l]&&(i=O(i)),{main:i,cross:O(i)}}(l,a,await (null==u.isRTL?void 0:u.isRTL(f.floating)));E.push(T[e],T[t])}if(A=[...A,{placement:l,overflows:E}],!E.every(e=>e<=0)){let e=(null!=(r=null==(n=i.flip)?void 0:n.index)?r:0)+1,t=k[e];if(t)return{data:{index:e,overflows:A},reset:{placement:t}};let a="bottom";switch(h){case"bestFit":{let e=null==(o=A.map(e=>[e,e.overflows.filter(e=>e>0).reduce((e,t)=>e+t,0)]).sort((e,t)=>e[1]-t[1])[0])?void 0:o[0].placement;e&&(a=e);break}case"initialPlacement":a=c}if(l!==a)return{reset:{placement:a}}}return{}}}),{name:"shift",options:i={padding:5},async fn(e){let{x:t,y:r,placement:n}=e,{mainAxis:o=!0,crossAxis:l=!1,limiter:a={fn:e=>{let{x:t,y:r}=e;return{x:t,y:r}}},...s}=i,c={x:t,y:r},u=await _(e,s),f=v(m(n)),p="x"===f?"y":"x",d=c[f],y=c[p];o&&(d=k(d+u["y"===f?"top":"left"],R(d,d-u["y"===f?"bottom":"right"]))),l&&(y=k(y+u["y"===p?"top":"left"],R(y,y-u["y"===p?"bottom":"right"])));let h=a.fn({...e,[f]:d,[p]:y});return{...h,data:{x:h.x-t,y:h.y-r}}}}];return r?(u.push({name:"arrow",options:c={element:r,padding:5},async fn(e){let{element:t,padding:r=0}=null!=c?c:{},{x:n,y:o,placement:l,rects:i,platform:a}=e;if(null==t)return console.warn("Floating UI: No `element` was passed to the `arrow` middleware."),{};let s=x(r),u={x:n,y:o},f=v(l),p=g(l),d=w(f),y=await a.getDimensions(t),h="y"===f?"top":"left",m="y"===f?"bottom":"right",b=i.reference[d]+i.reference[f]-u[f]-i.floating[d],S=u[f]-i.reference[f],_=await (null==a.getOffsetParent?void 0:a.getOffsetParent(t)),T=_?"y"===f?_.clientHeight||0:_.clientWidth||0:0;0===T&&(T=i.floating[d]);let O=s[h],E=T-y[d]-s[m],j=T/2-y[d]/2+(b/2-S/2),A=k(O,R(j,E)),P=("start"===p?s[h]:s[m])>0&&j!==A&&i.reference[d]<=i.floating[d];return{[f]:u[f]-(P?j<O?O-j:E-j:0),data:{[f]:A,centerOffset:j-A}}}}),et(e,t,{placement:n,strategy:l,middleware:u}).then(({x:e,y:t,placement:r,middlewareData:n})=>{var o,l;let i={left:`${e}px`,top:`${t}px`},{x:a,y:s}=null!==(o=n.arrow)&&void 0!==o?o:{x:0,y:0};return{tooltipStyles:i,tooltipArrowStyles:{left:null!=a?`${a}px`:"",top:null!=s?`${s}px`:"",right:"",bottom:"",[null!==(l=({top:"bottom",right:"left",bottom:"top",left:"right"})[r.split("-")[0]])&&void 0!==l?l:"bottom"]:"-4px"}}})):et(e,t,{placement:"bottom",strategy:l,middleware:u}).then(({x:e,y:t})=>({tooltipStyles:{left:`${e}px`,top:`${t}px`},tooltipArrowStyles:{}}))};var en={tooltip:"styles-module_tooltip__mnnfp",fixed:"styles-module_fixed__7ciUi",arrow:"styles-module_arrow__K0L3T","no-arrow":"styles-module_no-arrow__KcFZN",show:"styles-module_show__2NboJ",dark:"styles-module_dark__xNqje",light:"styles-module_light__Z6W-X",success:"styles-module_success__A2AKt",warning:"styles-module_warning__SCK0X",error:"styles-module_error__JvumD",info:"styles-module_info__BWdHW"};let eo=({id:e,className:t,classNameArrow:r,variant:l="dark",anchorId:i,place:a="top",offset:f=10,events:p=["hover"],positionStrategy:d="absolute",wrapper:h="div",children:m=null,delayShow:g=0,delayHide:v=0,float:w=!1,noArrow:b,style:x,position:S,isHtmlContent:_=!1,content:R,isOpen:k,setIsOpen:T})=>{let O=(0,n.useRef)(null),E=(0,n.useRef)(null),j=(0,n.useRef)(null),A=(0,n.useRef)(null),[P,N]=(0,n.useState)({}),[C,L]=(0,n.useState)({}),[$,D]=(0,n.useState)(!1),[W,F]=(0,n.useState)(!1),I=(0,n.useRef)(null),{anchorRefs:H,setActiveAnchor:B}=y()(e),[M,U]=(0,n.useState)({current:null}),V=e=>{T?T(e):void 0===k&&D(e)},z=()=>{A.current&&clearTimeout(A.current),A.current=setTimeout(()=>{V(!1)},v)},Y=e=>{var t;if(!e)return;g?(j.current&&clearTimeout(j.current),j.current=setTimeout(()=>{V(!0)},g)):V(!0);let r=null!==(t=e.currentTarget)&&void 0!==t?t:e.target;U(e=>e.current===r?e:{current:r}),B({current:r}),A.current&&clearTimeout(A.current)},q=({x:e,y:t})=>{F(!0),er({place:a,offset:f,elementReference:{getBoundingClientRect:()=>({x:e,y:t,width:0,height:0,top:t,left:e,right:e,bottom:t})},tooltipReference:O.current,tooltipArrowReference:E.current,strategy:d}).then(e=>{F(!1),Object.keys(e.tooltipStyles).length&&N(e.tooltipStyles),Object.keys(e.tooltipArrowStyles).length&&L(e.tooltipArrowStyles)})},X=e=>{if(!e)return;let t={x:e.clientX,y:e.clientY};q(t),I.current=t},K=e=>{Y(e),v&&z()},J=e=>{var t;(null===(t=M.current)||void 0===t?void 0:t.contains(e.target))||D(!1)},Z=c(Y,50),G=c(()=>{v?z():V(!1),j.current&&clearTimeout(j.current)},50);(0,n.useEffect)(()=>{let e=new Set(H),t=document.querySelector(`[id='${i}']`);if(t&&(U(e=>e.current===t?e:{current:t}),e.add({current:t})),!e.size)return()=>null;let r=[];return p.find(e=>"click"===e)&&(window.addEventListener("click",J),r.push({event:"click",listener:K})),p.find(e=>"hover"===e)&&(r.push({event:"mouseenter",listener:Z},{event:"mouseleave",listener:G},{event:"focus",listener:Z},{event:"blur",listener:G}),w&&r.push({event:"mousemove",listener:X})),r.forEach(({event:t,listener:r})=>{e.forEach(e=>{var n;null===(n=e.current)||void 0===n||n.addEventListener(t,r)})}),()=>{window.removeEventListener("click",J),r.forEach(({event:t,listener:r})=>{e.forEach(e=>{var n;null===(n=e.current)||void 0===n||n.removeEventListener(t,r)})})}},[H,M,i,p,v,g]),(0,n.useEffect)(()=>{if(S)return q(S),()=>null;if(w)return I.current&&q(I.current),()=>null;let e=M.current;i&&(e=document.querySelector(`[id='${i}']`)),F(!0);let t=!0;return er({place:a,offset:f,elementReference:e,tooltipReference:O.current,tooltipArrowReference:E.current,strategy:d}).then(e=>{t&&(F(!1),Object.keys(e.tooltipStyles).length&&N(e.tooltipStyles),Object.keys(e.tooltipArrowStyles).length&&L(e.tooltipArrowStyles))}),()=>{t=!1}},[$,k,i,M,R,a,f,d,S]),(0,n.useEffect)(()=>()=>{j.current&&clearTimeout(j.current),A.current&&clearTimeout(A.current)},[]);let Q=Boolean(R||m);return o.exports.jsxs(h,{id:e,role:"tooltip",className:s(en.tooltip,en[l],t,{[en.show]:Q&&!W&&(k||$),[en.fixed]:"fixed"===d}),style:{...x,...P},ref:O,children:[m||(_?o.exports.jsx(u,{content:R}):R),o.exports.jsx("div",{className:s(en.arrow,r,{[en["no-arrow"]]:b}),style:C,ref:E})]})},el=({id:e,anchorId:t,content:r,html:l,className:i,classNameArrow:a,variant:s="dark",place:c="top",offset:u=10,wrapper:f="div",children:p=null,events:d=["hover"],positionStrategy:h="absolute",delayShow:m=0,delayHide:g=0,float:v=!1,noArrow:w,style:b,position:x,isOpen:S,setIsOpen:_})=>{let[R,k]=(0,n.useState)(r||l),[T,O]=(0,n.useState)(c),[E,j]=(0,n.useState)(s),[A,P]=(0,n.useState)(u),[N,C]=(0,n.useState)(m),[L,$]=(0,n.useState)(g),[D,W]=(0,n.useState)(v),[F,I]=(0,n.useState)(f),[H,B]=(0,n.useState)(d),[M,U]=(0,n.useState)(h),[V,z]=(0,n.useState)(Boolean(l)),{anchorRefs:Y,activeAnchor:q}=y()(e),X=e=>null==e?void 0:e.getAttributeNames().reduce((t,r)=>{var n;return r.startsWith("data-tooltip-")&&(t[r.replace(/^data-tooltip-/,"")]=null!==(n=null==e?void 0:e.getAttribute(r))&&void 0!==n?n:null),t},{}),K=e=>{let t={place:e=>{O(null!=e?e:c)},content:e=>{k(null!=e?e:r)},html:e=>{var t;z(Boolean(null!=e?e:l)),k(null!==(t=null!=e?e:l)&&void 0!==t?t:r)},variant:e=>{j(null!=e?e:s)},offset:e=>{P(null===e?u:Number(e))},wrapper:e=>{I(null!=e?e:"div")},events:e=>{let t=null==e?void 0:e.split(" ");B(null!=t?t:d)},"position-strategy":e=>{U(null!=e?e:h)},"delay-show":e=>{C(null===e?m:Number(e))},"delay-hide":e=>{$(null===e?g:Number(e))},float:e=>{W(null===e?v:Boolean(e))}};Object.values(t).forEach(e=>e(null)),Object.entries(e).forEach(([e,r])=>{var n;null===(n=t[e])||void 0===n||n.call(t,r)})};(0,n.useEffect)(()=>{z(!1),k(r),l&&(z(!0),k(l))},[r,l]),(0,n.useEffect)(()=>{var e;let r=new Set(Y),n=document.querySelector(`[id='${t}']`);if(n&&r.add({current:n}),!r.size)return()=>null;let o=new MutationObserver(e=>{e.forEach(e=>{var t;if(!q.current||"attributes"!==e.type||!(null===(t=e.attributeName)||void 0===t?void 0:t.startsWith("data-tooltip-")))return;let r=X(q.current);K(r)})}),l=null!==(e=q.current)&&void 0!==e?e:n;if(l){let e=X(l);K(e),o.observe(l,{attributes:!0,childList:!1,subtree:!1})}return()=>{o.disconnect()}},[Y,q,t]);let J={id:e,anchorId:t,className:i,classNameArrow:a,content:R,isHtmlContent:V,place:T,variant:E,offset:A,wrapper:F,events:H,positionStrategy:M,delayShow:N,delayHide:L,float:D,noArrow:w,style:b,position:x,isOpen:S,setIsOpen:_};return p?o.exports.jsx(eo,{...J,children:p}):o.exports.jsx(eo,{...J})}}}]);