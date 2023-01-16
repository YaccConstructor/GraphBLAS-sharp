namespace GraphBLAS.FSharp.Backend.Quotes

module Predicates =
    let isSome<'a> =
        <@ fun (item: 'a option) ->
            match item with
            | Some _ -> true
            | _ -> false @>
