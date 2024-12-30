# thalloo maps for holocene arctic biodiversity database

A static github pages website based on the thalloo mapping template from andrewiom/thalloo-starter.

The dotnet project in `processing` transforms data in the graph database into flat tab-delimited text files, which may be read by the thalloo mapping static site. The processor is run by GitHub Actions when the graph is updated on the main branch, such that the thalloo static site is automatically updated.