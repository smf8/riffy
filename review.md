# Code review points:
- For every method/struct the priority is simplicity. Don't add unnecessary generic/type parameters. The code MUST be easy to understand for non rust experts.
an example would be Consumer<C, S> or pub fn new<I, S>(patterns: I) -> Self
Scan the whole project for similar instances and fix them.
- In consumer, check the http status code before comparing the JSON bodies. If the status is different we don't need to process bodies (find difference). We can report it directly.
It means that the analyzer don't need to accept Option for input parameters
- the proxy module is now responsible for both all application incoming requests and actual proxying. this might be confusing. create a dedicated handler module for http input interactions
- The project should have a Dockerfile to create a 100% production ready image with all security and performance best practices
- The redis module is responsible for the actual model definition AND redis AND in memory storage solutions. isn't it better to have these at more general module (fix this based on best practices you might know)?
- Can you use an already existing diff crate for compare module? spin up a search sub agent and search the feasibility.