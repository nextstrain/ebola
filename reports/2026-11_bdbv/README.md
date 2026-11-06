# BDBV blogpost built using `gazette`

# Prerequisites

1. Make sure `gazette` is on your path


# Build the report using `gazette`

```sh
# slow (~1min) - generate static fallback images
gazette snapshot 2026-11_bdbv.md
# PDF output
gazette build 2026-11_bdbv.md --target pdf

# static-site HTML
gazette build 2026-11_bdbv.md --target static

# serve locally
npx serve dist/static/
```

Or you can use dev-mode with live reloading as you edit the markdown:

```sh
gazette dev 2026-11_bdbv.md
```



# Re-building Auspice if needed 


From the Auspice source directory:
```sh
git checkout embed-render
npm run build:embed # produces embed-dist/auspice.js
```

The copy the produced `embed-dist/auspice.js` to this directory's `figures/lib/auspice.js`
(i.e. `reports/2026-11_bdbv/figures/lib/auspice.js`)


# Updating the Auspice dataset

Run the phylo workflow, replace the files in `data/` and update the data filenames in the markdown file.