namespace GraphBLAS.FSharp.Backend.Quotes

open Brahma.FSharp

module Predicates =
    let isSome<'a> =
        <@ fun (item: 'a option) ->
            match item with
            | Some _ -> true
            | _ -> false @>

    let inline lastOccurrence () =
        <@ fun (gid: int) (length: int) (inputArray: ClArray<'a>) ->
                  gid = length - 1
                  || inputArray.[gid] <> inputArray.[gid + 1] @>

    let inline firstOccurrence () =
        <@ fun (gid: int) (_: int) (inputArray: ClArray<'a>) ->
                  gid = 0
                  || inputArray.[gid - 1] <> inputArray.[gid] @>
