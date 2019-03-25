module Text.Wrap
( dedent
, indent
) where

import Prelude
import Data.Array as A
import Data.String as Str
import Data.String (Pattern(..))
import Data.String.CodeUnits (toCharArray)
import Data.Maybe (fromMaybe)

indent :: String -> String
indent txt =
  let
    lines :: Array String
    lines = Str.split (Pattern "\n") txt
  in
  Str.joinWith "\n" $ map ("  " <> _) lines

dedent :: String -> String
dedent txt =
  let
    lines :: Array String
    lines = Str.split (Pattern "\n") txt
    nonEmpty :: String -> Boolean
    nonEmpty = (_ /= 0) <<< Str.length <<< Str.trim
    shortestLeading :: Int
    shortestLeading = fromMaybe 0 (A.head $ A.sort $ countLeading
                            <$> (A.filter nonEmpty lines))
    isWhitespace :: Char -> Boolean
    isWhitespace ' '  = true
    isWhitespace '\n' = true
    isWhitespace '\t' = true
    isWhitespace _    = false
    countLeading :: String -> Int
    countLeading line = A.length $ A.takeWhile isWhitespace (toCharArray line)
  in
    Str.joinWith "\n" ((Str.drop shortestLeading) <$> lines)

