const makeQuerier = () => {
    const allSections = [...document
        .getElementsByClassName("resource-list")[0]
        .querySelectorAll("section")
    ];

    // TODO 2024-09-16: try having the core document be a *column* not a whole
    // table. and then - when rendering, show the table description + relevant columns.
    const documents = allSections.map(section => {
        return {
            id: section.id,
            title: section.getElementsByTagName("h2")[0].innerText,
            text: section.innerText,
            node: section
        }
    });

    // TODO 2024-09-16: tokenize better - split between source & form #; split underscores
    // TODO 2024-09-16: add stopwords & stemming
    const miniSearch = new MiniSearch({
        fields: ["title", "text"],
        storeFields: ["id", "node"],
    });

    miniSearch.addAll(documents);

    const query = e => {
        const results = miniSearch.search(
            // boost the out layer, unless core or raw are in the query
            e.target.value,
            {
                prefix: true,
                fuzzy: 0.2,
            }
        );
        renderQueryResults(results);
    };

    const debounce = (callback, waitTime) => {
        let timer;
        return (...args) => {
            clearTimeout(timer);
            timer = setTimeout(() => {
                callback(...args);
            }, waitTime);
        };
    }

    const debounced = debounce(query, 200);
    return debounced;
};

const renderQueryResults = results => {
    const resourceList = document.getElementsByClassName("resource-list")[0];
    const resultSections = results.map(s => s.node);
    resourceList.replaceChildren(...resultSections);

    // append the title to the list of result titles
    // and make the .toc-tree only have those titles
}


window.onload = () => {
    const runQuery = makeQuerier()
    const input = document.getElementById("search-input");
    input.oninput = runQuery;
}
