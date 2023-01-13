namespace GraphBLAS.FSharp.Backend.Quotes

module Predicates =
    let containsNonZero<'a> =
        <@ fun (item: 'a option) ->
            match item with
            | Some _ -> true
            | _ -> false @>
