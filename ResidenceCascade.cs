namespace Thoth.Connectors.Csv;

/// <summary>
/// Where a residence comes from, from the weakest to the strongest term of the cascade. The numeric
/// order IS the precedence: two origins compare with &lt; and &gt;.
/// </summary>
internal enum ResidenceOrigin
{
    /// <summary>The row says nothing: the engine backfills its default scope.</summary>
    None = 0,

    /// <summary>Derived from the school entered for the file.</summary>
    FileSchool = 1,

    /// <summary>Stated by the transverse district setting (<c>residence_district</c>).</summary>
    TransverseDistrict = 2,

    /// <summary>Stated by the row itself, in the column mapped to the residence.</summary>
    Column = 3
}

/// <summary>A residence and the term of the cascade it comes from.</summary>
internal readonly record struct ResolvedResidence(string? Value, ResidenceOrigin Origin);

/// <summary>
/// The residence cascade (mapped column &gt; transverse district &gt; school of the file &gt; nothing),
/// applied to ONE row (<see cref="Resolve"/>) and to ONE person folded from several rows
/// (<see cref="Fold"/>). Applying it to the person is what makes cross-referencing independent of
/// the order the files are read in: a residence column mapped in one file wins over the school
/// entered for another file, whether it is read before or after it.
///
/// Pure logic, deliberately free of any dependency on the rest of the connector, so it can be
/// tested on its own.
/// </summary>
internal static class ResidenceCascade
{
    /// <summary>
    /// The residence of ONE row. The blank check serves ONLY the choice between the cell and what
    /// follows: the retained value travels RAW, and the engine sanitizes it.
    /// <paramref name="transverseDistrict"/> arrives already cleaned (null or trimmed), and only for
    /// teachers and staff.
    /// </summary>
    public static ResolvedResidence Resolve(string? cell, string? transverseDistrict, string? fileSchool)
    {
        if (!string.IsNullOrWhiteSpace(cell)) return new(cell, ResidenceOrigin.Column);
        if (transverseDistrict is not null) return new(transverseDistrict, ResidenceOrigin.TransverseDistrict);
        if (fileSchool is not null) return new(fileSchool, ResidenceOrigin.FileSchool);
        return new(null, ResidenceOrigin.None);
    }

    /// <summary>
    /// The residence of ONE person, from the one kept so far and the one of the row just read. The
    /// stronger origin wins whatever the order; a weaker one is dropped without a word, as within a
    /// row. Between two values of the SAME origin the first read stays (the rule of every scalar),
    /// and <c>Diverged</c> says they disagree. They are compared trimmed and case-insensitively:
    /// the engine sanitizes and uppercases, so padding and case are not a disagreement.
    /// </summary>
    public static (ResolvedResidence Kept, bool Diverged) Fold(ResolvedResidence kept, ResolvedResidence incoming)
    {
        if (incoming.Origin > kept.Origin) return (incoming, false);
        if (incoming.Origin < kept.Origin || kept.Origin == ResidenceOrigin.None) return (kept, false);

        bool diverged = !kept.Value!.Trim().Equals(incoming.Value!.Trim(), StringComparison.OrdinalIgnoreCase);
        return (kept, diverged);
    }
}
