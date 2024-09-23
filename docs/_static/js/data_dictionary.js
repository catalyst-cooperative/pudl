const makeQuerier = () => {
    const allSections = [...document
        .getElementsByClassName("resource-list")[0]
        .querySelectorAll("section")
    ];

    const tocNodeMap = [
        ...document.querySelector(".toc-tree li ul").children
    ].reduce((acc, cur) => { acc[cur.innerText] = cur; return acc }, {});
    // TODO 2024-09-17: long-term, it probably makes sense to just jam a bunch
    // of structured data into the `package.rst.jinja` as a JSON.

    // TODO 2024-09-16: try having the core document be a *column* not a whole
    // table. and then - when rendering, show the table description + relevant columns.
    const documents = allSections.map(section => {
        const title = section.getElementsByTagName("h2")[0].innerText;
        return {
            id: section.id,
            title: title,
            text: section.innerText,
            node: section,
            tocNode: tocNodeMap[title]
        }
    });

    // TODO 2024-09-16: add stopwords & stemming
    const miniSearch = new MiniSearch({
        fields: ["title", "text"],
        storeFields: ["id", "node", "tocNode"],
        tokenize: (string, _fieldName) => string.replace(/(\d+)/g, "_$1").split(/[^a-zA-Z0-9]/)
    });

    miniSearch.addAll(documents);

    const query = e => {
        const rawQuery = e.target.value.toLowerCase();
        let results = [];
        if (rawQuery.trim() === "") {
            results = allSections.map(section => { node: section });
        }

        let query = rawQuery;

        // TODO: actually parse out the layer as a structured field instead of searching like this.
        if (rawQuery.search(/raw|core|out/) === -1) {
            query += " out";
        }

        results = miniSearch.search(
            query,
            {
                prefix: true,
                fuzzy: 0.2,
                combineWith: "and",
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
    /**
     * TODO 2024-09-17: use the result.match MatchInfo object to highlight the matching text:
     * - find all the terms that matched (e.g. generator, generation, generated)
     * - run a replaceAll on the innerHTML, wrapping all instances with <span class="highlighted"></span>
     * - replace the innerHTML
     * - make sure there's CSS that highlights the highlighted
     */
    const resourceList = document.getElementsByClassName("resource-list")[0];
    const resultSections = results.map(s => s.node);
    const resultTocNodes = results.map(s => s.tocNode);
    resourceList.replaceChildren(...resultSections);
    const tocList = document.querySelector(".toc-tree li ul");
    tocList.replaceChildren(...resultTocNodes);
}


window.onload = () => {
    const runQuery = makeQuerier()
    const input = document.getElementById("search-input");
    input.oninput = runQuery;
}
