CREATE TABLE IF EXISTS obitos (
        /* CAUSA */
        COVID INTEGER NOT NULL DEFAULT 0,
        SRAG INTEGER NOT NULL DEFAULT 0,
        PNEUMONIA INTEGER NOT NULL DEFAULT 0,
        INSUFICIENCIA_RESPIRATORIA INTEGER NOT NULL DEFAULT 0,
        SEPTICEMIA INTEGER NOT NULL DEFAULT 0,
        INDETERMINADA INTEGER NOT NULL DEFAULT 0,
        OUTRAS INTEGER NOT NULL DEFAULT 0,

        /* LOCAL */
        CIDADE TEXT,
        ESTADO TEXT,
        LUGAR TEXT,

        /* DATA */
        DIA DATE
);