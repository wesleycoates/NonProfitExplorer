--
-- PostgreSQL database dump
--

\restrict l4S6pxqD2wH0d5vwA3mQz5t8eMTdwWZeCYaOKcNGycKcVoe68IUUTtxVcV2gwGN

-- Dumped from database version 15.17 (Debian 15.17-1.pgdg12+1)
-- Dumped by pg_dump version 15.17 (Debian 15.17-1.pgdg12+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- Name: uuid-ossp; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;


--
-- Name: EXTENSION "uuid-ossp"; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: ai_analyses; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ai_analyses (
    id bigint NOT NULL,
    filing_id bigint,
    ein character varying(10) NOT NULL,
    tax_prd_yr smallint,
    model character varying(60) NOT NULL,
    insights jsonb,
    outlook text,
    flags jsonb,
    raw_response text,
    tokens_in integer,
    tokens_out integer,
    cost_usd numeric(10,6),
    analyzed_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.ai_analyses OWNER TO postgres;

--
-- Name: ai_analyses_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.ai_analyses_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ai_analyses_id_seq OWNER TO postgres;

--
-- Name: ai_analyses_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.ai_analyses_id_seq OWNED BY public.ai_analyses.id;


--
-- Name: filings; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.filings (
    id bigint NOT NULL,
    ein character varying(10) NOT NULL,
    tax_prd character varying(6) NOT NULL,
    tax_prd_yr smallint GENERATED ALWAYS AS (("left"((tax_prd)::text, 4))::smallint) STORED,
    form_type character varying(10),
    org_name text,
    address text,
    city character varying(100),
    state character(2),
    zipcode character varying(10),
    totrevenue bigint,
    totfuncexpns bigint,
    totassetsend bigint,
    totliabend bigint,
    netassetsend bigint,
    prgmservrev bigint,
    investinc bigint,
    othrevnue bigint,
    grscontribs bigint,
    fedgrnts bigint,
    totprgmexpns bigint,
    totgrants bigint,
    totmgmtexpns bigint,
    totfndrsng bigint,
    compnsatncurrofcr bigint,
    othrsalwages bigint,
    payrolltx bigint,
    noemployees integer,
    noemplyeesw2 integer,
    noindcontractor integer,
    surplus bigint GENERATED ALWAYS AS ((totrevenue - totfuncexpns)) STORED,
    pdf_url text,
    source_url text,
    raw_json jsonb,
    ingested_at timestamp with time zone DEFAULT now(),
    margin_pct numeric GENERATED ALWAYS AS (
CASE
    WHEN ((totrevenue IS NOT NULL) AND (totrevenue <> 0)) THEN round(((((totrevenue - totfuncexpns))::numeric / (totrevenue)::numeric) * (100)::numeric), 2)
    ELSE NULL::numeric
END) STORED,
    program_rev_pct numeric GENERATED ALWAYS AS (
CASE
    WHEN ((totrevenue IS NOT NULL) AND (totrevenue <> 0)) THEN round((((COALESCE(prgmservrev, (0)::bigint))::numeric / (totrevenue)::numeric) * (100)::numeric), 2)
    ELSE NULL::numeric
END) STORED,
    royaltsinc bigint,
    miscrevtot bigint,
    grsincfndrsng bigint,
    netincfndrsng bigint,
    profndraising bigint,
    grsincgaming bigint,
    netincgaming bigint,
    netrntlinc bigint,
    txexmptbndsend bigint,
    unrelbusinc boolean,
    nonpfrea character varying(10)
);


ALTER TABLE public.filings OWNER TO postgres;

--
-- Name: filings_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.filings_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.filings_id_seq OWNER TO postgres;

--
-- Name: filings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.filings_id_seq OWNED BY public.filings.id;


--
-- Name: organizations; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.organizations (
    ein character varying(10) NOT NULL,
    name text NOT NULL,
    name_normalized text GENERATED ALWAYS AS (lower(TRIM(BOTH FROM name))) STORED,
    city character varying(100),
    state character(2),
    zipcode character varying(10),
    ntee_code character varying(10),
    ntee_major smallint,
    subseccd character varying(5),
    ruling_date character varying(6),
    asset_cd smallint,
    income_cd smallint,
    affiliation smallint,
    classification character varying(10),
    deductibility smallint,
    foundation smallint,
    activity character varying(10),
    organization smallint,
    status smallint,
    tax_period character varying(6),
    acct_pd smallint,
    filing_req_cd smallint,
    sort_name text,
    first_seen_at timestamp with time zone DEFAULT now(),
    last_refreshed_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.organizations OWNER TO postgres;

--
-- Name: personnel; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.personnel (
    id integer NOT NULL,
    filing_id integer,
    ein character varying(10),
    tax_prd_yr smallint,
    person_name text,
    title text,
    compensation bigint,
    related_comp bigint,
    other_comp bigint,
    hours_per_week numeric(5,1),
    is_officer boolean,
    is_key_employee boolean,
    ingested_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.personnel OWNER TO postgres;

--
-- Name: personnel_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.personnel_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.personnel_id_seq OWNER TO postgres;

--
-- Name: personnel_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.personnel_id_seq OWNED BY public.personnel.id;


--
-- Name: pipeline_runs; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.pipeline_runs (
    id integer NOT NULL,
    pipeline character varying(30) NOT NULL,
    status character varying(20) DEFAULT 'running'::character varying NOT NULL,
    started_at timestamp with time zone DEFAULT now(),
    finished_at timestamp with time zone,
    orgs_processed integer DEFAULT 0,
    orgs_errored integer DEFAULT 0,
    notes text
);


ALTER TABLE public.pipeline_runs OWNER TO postgres;

--
-- Name: pipeline_runs_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.pipeline_runs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.pipeline_runs_id_seq OWNER TO postgres;

--
-- Name: pipeline_runs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.pipeline_runs_id_seq OWNED BY public.pipeline_runs.id;


--
-- Name: v_latest_filings; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.v_latest_filings AS
 SELECT DISTINCT ON (f.ein) o.name,
    o.city,
    o.state,
    o.ntee_code,
    f.ein,
    f.tax_prd_yr,
    f.form_type,
    f.totrevenue,
    f.totfuncexpns,
    f.surplus,
    f.margin_pct,
    f.totassetsend,
    f.netassetsend,
    f.noemployees,
    f.prgmservrev,
    f.program_rev_pct,
    a.insights,
    a.outlook,
    a.flags
   FROM ((public.filings f
     JOIN public.organizations o ON (((o.ein)::text = (f.ein)::text)))
     LEFT JOIN public.ai_analyses a ON ((a.filing_id = f.id)))
  WHERE ((f.totrevenue >= 10000000) AND ((f.margin_pct IS NULL) OR ((f.margin_pct >= ('-100'::integer)::numeric) AND (f.margin_pct <= (100)::numeric))))
  ORDER BY f.ein, f.tax_prd_yr DESC;


ALTER TABLE public.v_latest_filings OWNER TO postgres;

--
-- Name: v_revenue_trend; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.v_revenue_trend AS
 SELECT f.ein,
    o.name,
    o.state,
    f.tax_prd_yr,
    f.totrevenue,
    f.totfuncexpns,
    f.surplus,
    f.margin_pct,
    lag(f.totrevenue) OVER (PARTITION BY f.ein ORDER BY f.tax_prd_yr) AS prev_yr_revenue
   FROM (public.filings f
     JOIN public.organizations o ON (((o.ein)::text = (f.ein)::text)))
  WHERE (f.tax_prd_yr >= ((EXTRACT(year FROM now()))::smallint - 5));


ALTER TABLE public.v_revenue_trend OWNER TO postgres;

--
-- Name: ai_analyses id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ai_analyses ALTER COLUMN id SET DEFAULT nextval('public.ai_analyses_id_seq'::regclass);


--
-- Name: filings id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.filings ALTER COLUMN id SET DEFAULT nextval('public.filings_id_seq'::regclass);


--
-- Name: personnel id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.personnel ALTER COLUMN id SET DEFAULT nextval('public.personnel_id_seq'::regclass);


--
-- Name: pipeline_runs id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pipeline_runs ALTER COLUMN id SET DEFAULT nextval('public.pipeline_runs_id_seq'::regclass);


--
-- Name: ai_analyses ai_analyses_filing_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ai_analyses
    ADD CONSTRAINT ai_analyses_filing_id_key UNIQUE (filing_id);


--
-- Name: ai_analyses ai_analyses_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ai_analyses
    ADD CONSTRAINT ai_analyses_pkey PRIMARY KEY (id);


--
-- Name: filings filings_ein_tax_prd_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.filings
    ADD CONSTRAINT filings_ein_tax_prd_key UNIQUE (ein, tax_prd);


--
-- Name: filings filings_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.filings
    ADD CONSTRAINT filings_pkey PRIMARY KEY (id);


--
-- Name: organizations organizations_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.organizations
    ADD CONSTRAINT organizations_pkey PRIMARY KEY (ein);


--
-- Name: personnel personnel_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.personnel
    ADD CONSTRAINT personnel_pkey PRIMARY KEY (id);


--
-- Name: personnel personnel_unique_person; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.personnel
    ADD CONSTRAINT personnel_unique_person UNIQUE (ein, tax_prd_yr, person_name, title);


--
-- Name: pipeline_runs pipeline_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pipeline_runs
    ADD CONSTRAINT pipeline_runs_pkey PRIMARY KEY (id);


--
-- Name: idx_ai_ein; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_ai_ein ON public.ai_analyses USING btree (ein);


--
-- Name: idx_ai_tax_prd_yr; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_ai_tax_prd_yr ON public.ai_analyses USING btree (tax_prd_yr);


--
-- Name: idx_filings_ein; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_filings_ein ON public.filings USING btree (ein);


--
-- Name: idx_filings_form_type; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_filings_form_type ON public.filings USING btree (form_type);


--
-- Name: idx_filings_state; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_filings_state ON public.filings USING btree (state);


--
-- Name: idx_filings_tax_prd_yr; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_filings_tax_prd_yr ON public.filings USING btree (tax_prd_yr);


--
-- Name: idx_filings_totrevenue; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_filings_totrevenue ON public.filings USING btree (totrevenue);


--
-- Name: idx_org_asset_cd; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_org_asset_cd ON public.organizations USING btree (asset_cd);


--
-- Name: idx_org_name_trgm; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_org_name_trgm ON public.organizations USING gin (name_normalized public.gin_trgm_ops);


--
-- Name: idx_org_ntee_major; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_org_ntee_major ON public.organizations USING btree (ntee_major);


--
-- Name: idx_org_state; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_org_state ON public.organizations USING btree (state);


--
-- Name: idx_org_subseccd; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_org_subseccd ON public.organizations USING btree (subseccd);


--
-- Name: idx_personnel_compensation; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_personnel_compensation ON public.personnel USING btree (compensation DESC);


--
-- Name: idx_personnel_ein; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_personnel_ein ON public.personnel USING btree (ein);


--
-- Name: idx_personnel_filing_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_personnel_filing_id ON public.personnel USING btree (filing_id);


--
-- Name: ai_analyses ai_analyses_filing_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ai_analyses
    ADD CONSTRAINT ai_analyses_filing_id_fkey FOREIGN KEY (filing_id) REFERENCES public.filings(id) ON DELETE CASCADE;


--
-- Name: filings filings_ein_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.filings
    ADD CONSTRAINT filings_ein_fkey FOREIGN KEY (ein) REFERENCES public.organizations(ein) ON DELETE CASCADE;


--
-- Name: personnel personnel_filing_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.personnel
    ADD CONSTRAINT personnel_filing_id_fkey FOREIGN KEY (filing_id) REFERENCES public.filings(id) ON DELETE CASCADE;


--
-- Name: TABLE ai_analyses; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.ai_analyses TO nonprofit_user;


--
-- Name: SEQUENCE ai_analyses_id_seq; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON SEQUENCE public.ai_analyses_id_seq TO nonprofit_user;


--
-- Name: TABLE filings; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.filings TO nonprofit_user;


--
-- Name: SEQUENCE filings_id_seq; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON SEQUENCE public.filings_id_seq TO nonprofit_user;


--
-- Name: TABLE organizations; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.organizations TO nonprofit_user;


--
-- Name: TABLE personnel; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.personnel TO nonprofit_user;


--
-- Name: SEQUENCE personnel_id_seq; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT,USAGE ON SEQUENCE public.personnel_id_seq TO nonprofit_user;


--
-- Name: TABLE pipeline_runs; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.pipeline_runs TO nonprofit_user;


--
-- Name: SEQUENCE pipeline_runs_id_seq; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON SEQUENCE public.pipeline_runs_id_seq TO nonprofit_user;


--
-- PostgreSQL database dump complete
--

\unrestrict l4S6pxqD2wH0d5vwA3mQz5t8eMTdwWZeCYaOKcNGycKcVoe68IUUTtxVcV2gwGN

