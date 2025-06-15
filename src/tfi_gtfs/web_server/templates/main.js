// Initialise the form with the stop numbers from the query string
function initStopNumbers() {
    let stops = window.location.search.substring(1).split('&').filter(q => q.startsWith('stop=')).map(q => q.replace('stop=', ''));
    let stopNumber = 1;
    for (let stop of stops) {
        let input = document.getElementById(`stop${stopNumber}`);
        input.value = stop;
        stopNumber++;
    }
}

// Explicitly handle form submission so we can alter the accept header
function submitHandler(e) {
    const form = document.getElementById('form');
    e.preventDefault();
    const xhr = new XMLHttpRequest();
    const formData = new FormData(form);
    const accept = document.getElementById("accept").value;
    let url = form.getAttribute("action") + "?" + new URLSearchParams(formData).toString();
    // remove any empty stop query params
    if(url.indexOf('stop=&') != -1)
        url = url.replace('stop=&', '');
    if(url.endsWith("stop="))
        url = url.substring(0, url.length - 5);
    if(url.endsWith("&"))
        url = url.substring(0, url.length - 1);
    xhr.open(form.method, url);
    // set the accept header to the value of the accept query parameter
    xhr.setRequestHeader('Accept', accept);
    xhr.onload = () => {
        if (xhr.readyState === xhr.DONE && xhr.status === 200) {
            if(xhr.getResponseHeader("content-type").indexOf('text/html') != -1) {
                document.body.innerHTML = xhr.response;
                // update the window location so that the query string is updated
                window.history.pushState({}, '', url);
                initStopNumbers();
                document.getElementById('form').addEventListener('submit', submitHandler);
            }
            else {
                document.body.innerHTML = `<pre>${xhr.response}</pre>`;
            }
        }
    };
    xhr.send();
    return false;
}
initStopNumbers();
document.getElementById('form').addEventListener('submit', submitHandler);

