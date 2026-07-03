# RxJS

RxJS is a library for reactive programming using observables. Observables
represent streams of values over time. Operators compose streams: map,
filter, mergeMap, concatMap. Subscriptions are cancellable, and teardown
releases resources deterministically.

Cold observables start work per subscriber. Hot observables share one
underlying execution among subscribers.
